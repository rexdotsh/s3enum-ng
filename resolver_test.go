package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func startTestResolver(t testing.TB) (*net.UDPConn, string, *sync.WaitGroup) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 2048)
		for {
			count, peer, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			message := append([]byte(nil), buffer[:count]...)
			question, _, err := decodeName(message, dnsHeaderLen)
			if err != nil {
				continue
			}
			target := nonexistentTarget
			if question == "exists."+s3Suffix {
				target = "s3-eu-west-1-w.amazonaws.com."
			}
			encodedTarget, err := encodeName(target)
			if err != nil {
				continue
			}
			binary.BigEndian.PutUint16(message[2:4], 0x8180)
			binary.BigEndian.PutUint16(message[6:8], 1)
			answer := make([]byte, 12+len(encodedTarget))
			binary.BigEndian.PutUint16(answer[0:2], 0xc00c)
			binary.BigEndian.PutUint16(answer[2:4], dnsTypeCNAME)
			binary.BigEndian.PutUint16(answer[4:6], dnsClassIN)
			binary.BigEndian.PutUint16(answer[10:12], uint16(len(encodedTarget)))
			copy(answer[12:], encodedTarget)
			_, _ = conn.WriteToUDP(append(message, answer...), peer)
		}
	}()
	return conn, conn.LocalAddr().String(), &wg
}

func TestRunnerEndToEnd(t *testing.T) {
	server, address, serverWG := startTestResolver(t)
	defer func() {
		_ = server.Close()
		serverWG.Wait()
	}()

	var output bytes.Buffer
	runner, err := newRunner(resolverConfig{
		resolvers:   []string{address},
		sockets:     2,
		concurrency: 32,
		timeout:     100 * time.Millisecond,
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(context.Background(), "exists"); err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(context.Background(), "missing"); err != nil {
		t.Fatal(err)
	}
	runner.wg.Wait()
	if got := output.String(); got != "exists\n" {
		t.Fatalf("output before shutdown = %q, want %q", got, "exists\\n")
	}
	if err := runner.finish(); err != nil {
		t.Fatal(err)
	}

	if got := output.String(); got != "exists\n" {
		t.Fatalf("output = %q, want %q", got, "exists\\n")
	}
	stats := runner.snapshot()
	if stats.Checked != 2 || stats.Existing != 1 || stats.Found != 1 || stats.Errors != 0 || stats.Queries != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRunnerRetriesAndCountsFinalTimeout(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runner, err := newRunner(resolverConfig{
		resolvers:   []string{server.LocalAddr().String()},
		sockets:     1,
		concurrency: 1,
		timeout:     20 * time.Millisecond,
		retries:     1,
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(context.Background(), "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := runner.finish(); err != nil {
		t.Fatal(err)
	}

	stats := runner.snapshot()
	if stats.Checked != 1 || stats.Existing != 0 || stats.Found != 0 || stats.Errors != 1 || stats.Queries != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRunnerRetriesOnAnotherResolver(t *testing.T) {
	silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()

	server, address, serverWG := startTestResolver(t)
	defer func() {
		_ = server.Close()
		serverWG.Wait()
	}()

	var output bytes.Buffer
	runner, err := newRunner(resolverConfig{
		resolvers:   []string{silent.LocalAddr().String(), address},
		sockets:     1,
		concurrency: 1,
		timeout:     20 * time.Millisecond,
		retries:     1,
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(context.Background(), "exists"); err != nil {
		t.Fatal(err)
	}
	if err := runner.finish(); err != nil {
		t.Fatal(err)
	}

	if got := output.String(); got != "exists\n" {
		t.Fatalf("output = %q", got)
	}
	stats := runner.snapshot()
	if stats.Queries != 2 || stats.Errors != 0 || stats.Found != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRunnerCancellationIsNotAnOperationalError(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runner, err := newRunner(resolverConfig{
		context:     ctx,
		resolvers:   []string{server.LocalAddr().String()},
		sockets:     1,
		concurrency: 1,
		timeout:     time.Second,
		retries:     1,
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(ctx, "pending"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := runner.abort(); err != nil {
		t.Fatal(err)
	}

	stats := runner.snapshot()
	if stats.Errors != 0 || stats.Canceled != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRunnerStopsOnOutputFailure(t *testing.T) {
	server, address, serverWG := startTestResolver(t)
	defer func() {
		_ = server.Close()
		serverWG.Wait()
	}()

	runner, err := newRunner(resolverConfig{
		resolvers:   []string{address},
		sockets:     1,
		concurrency: 4,
		timeout:     100 * time.Millisecond,
	}, errorWriter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.submit(context.Background(), "exists"); err != nil {
		t.Fatal(err)
	}
	if err := runner.finish(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("finish error = %v, want closed pipe", err)
	}
	if stats := runner.snapshot(); stats.Found != 0 {
		t.Fatalf("failed output was counted as found: %+v", stats)
	}
}

func TestResolverAddress(t *testing.T) {
	tests := map[string]string{
		"1.1.1.1":       "1.1.1.1:53",
		"1.1.1.1:5353":  "1.1.1.1:5353",
		"[2001:db8::1]": "[2001:db8::1]:53",
	}
	for input, want := range tests {
		address, err := resolverAddress(input)
		if err != nil {
			t.Fatalf("resolverAddress(%q): %v", input, err)
		}
		if got := address.String(); got != want {
			t.Errorf("resolverAddress(%q) = %q, want %q", input, got, want)
		}
	}
}

func BenchmarkRunnerLoopback(b *testing.B) {
	server, address, serverWG := startTestResolver(b)
	defer func() {
		_ = server.Close()
		serverWG.Wait()
	}()

	runner, err := newRunner(resolverConfig{
		resolvers:   []string{address},
		sockets:     1,
		concurrency: 128,
		timeout:     time.Second,
	}, &bytes.Buffer{})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.submit(context.Background(), "missing"); err != nil {
			b.Fatal(err)
		}
	}
	if err := runner.finish(); err != nil {
		b.Fatal(err)
	}
	if stats := runner.snapshot(); stats.Errors != 0 {
		b.Fatalf("benchmark lost %d of %d queries", stats.Errors, stats.Checked)
	}
}
