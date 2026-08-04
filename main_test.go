package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunWritesBucketsAndStatsToSeparateStreams(t *testing.T) {
	server, address, serverWG := startTestResolver(t)
	defer func() {
		_ = server.Close()
		serverWG.Wait()
	}()

	dir := t.TempDir()
	wordlist := filepath.Join(dir, "words.txt")
	suffixlist := filepath.Join(dir, "suffixes.txt")
	if err := os.WriteFile(wordlist, []byte("word\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(suffixlist, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"-wordlist", wordlist,
		"-suffixlist", suffixlist,
		"-resolver", address,
		"-sockets", "1",
		"-workers", "16",
		"-timeout", "100ms",
		"-retries", "0",
		"exists",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", exitCode, stderr.String())
	}
	if got := stdout.String(); got != "exists\n" {
		t.Fatalf("stdout = %q, want %q", got, "exists\\n")
	}
	for _, value := range []string{"checked: 9", "found: 1", "errors: 0", "packets: 9", "packets/sec:"} {
		if !strings.Contains(stderr.String(), value) {
			t.Errorf("stderr %q does not contain %q", stderr.String(), value)
		}
	}
}

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exitCode := run([]string{"-version"}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", exitCode, stderr.String())
	}
	if got := stdout.String(); got != currentVersion()+"\n" {
		t.Fatalf("version output = %q", got)
	}
}

func TestRunHelpReturnsSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exitCode := run([]string{"-h"}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage: s3enum-ng") {
		t.Fatalf("help output = %q", stderr.String())
	}
}

func TestRunValidatesInputsBeforeNetworkAccess(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	dir := t.TempDir()
	suffixlist := filepath.Join(dir, "suffixes.txt")
	if err := os.WriteFile(suffixlist, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	missingWordlist := filepath.Join(dir, "missing-words.txt")

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"-wordlist", missingWordlist,
		"-suffixlist", suffixlist,
		"-resolver", server.LocalAddr().String(),
		"target",
	}, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "open word list") {
		t.Fatalf("exit code %d, stderr: %q", exitCode, stderr.String())
	}

	if err := server.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.ReadFromUDP(make([]byte, 512)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("unexpected network request before input validation: %v", err)
	}
}

func TestRunReturnsFailureForIncompleteScan(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	dir := t.TempDir()
	wordlist := filepath.Join(dir, "words.txt")
	suffixlist := filepath.Join(dir, "suffixes.txt")
	for _, path := range []string{wordlist, suffixlist} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"-wordlist", wordlist,
		"-suffixlist", suffixlist,
		"-resolver", server.LocalAddr().String(),
		"-sockets", "1",
		"-concurrency", "1",
		"-timeout", "10ms",
		"-retries", "0",
		"missing",
	}, &stdout, &stderr)
	if exitCode != 1 {
		t.Fatalf("exit code %d, stderr: %s", exitCode, stderr.String())
	}
	for _, value := range []string{"checked: 1", "errors: 1", "DNS query timed out"} {
		if !strings.Contains(stderr.String(), value) {
			t.Errorf("stderr %q does not contain %q", stderr.String(), value)
		}
	}
}

func TestCurrentVersionFormatting(t *testing.T) {
	original := version
	t.Cleanup(func() { version = original })

	for input, want := range map[string]string{
		"1.2.3":  "v1.2.3",
		"v1.2.3": "v1.2.3",
		"devel":  "devel",
	} {
		version = input
		if got := currentVersion(); got != want {
			t.Errorf("currentVersion() with %q = %q, want %q", input, got, want)
		}
	}
}

func TestWorkersCompatibilityFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"-workers", "0",
		"-wordlist", "words.txt",
		"-suffixlist", "suffixes.txt",
		"target",
	}, &stdout, &stderr)
	if exitCode != 2 || !strings.Contains(stderr.String(), "workers must be a positive number") {
		t.Fatalf("exit code %d, stderr: %q", exitCode, stderr.String())
	}
}
