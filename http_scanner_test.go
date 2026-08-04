package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

func TestHTTPScannerFiltersAndCountsResults(t *testing.T) {
	probe := func(_ context.Context, candidate string) (bool, bool, error) {
		switch candidate {
		case "public":
			return true, true, nil
		case "private":
			return true, false, nil
		case "error":
			return true, false, errors.New("listing failed")
		default:
			return false, false, nil
		}
	}

	var output bytes.Buffer
	scanner := newHTTPScanner(context.Background(), probe, 4, &output)
	for _, candidate := range []string{"public", "private", "missing", "error"} {
		if err := scanner.submit(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.finish(); err != nil {
		t.Fatal(err)
	}

	if got := output.String(); got != "public\n" {
		t.Fatalf("output = %q, want %q", got, "public\\n")
	}
	stats := scanner.snapshot()
	if stats.Checked != 4 || stats.Existing != 3 || stats.Found != 1 || stats.Errors != 1 || stats.Canceled != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestHTTPScannerCancelsOnOutputFailure(t *testing.T) {
	var probes atomic.Int32
	probe := func(_ context.Context, _ string) (bool, bool, error) {
		probes.Add(1)
		return true, true, nil
	}

	scanner := newHTTPScanner(context.Background(), probe, 2, errorWriter{})
	for i := 0; i < 100; i++ {
		if err := scanner.submit(context.Background(), "candidate"); err != nil {
			break
		}
	}
	if err := scanner.finish(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("finish error = %v, want closed pipe", err)
	}
	if got := probes.Load(); got >= 100 {
		t.Fatalf("output failure did not stop probes: got %d", got)
	}
	if stats := scanner.snapshot(); stats.Canceled == 0 {
		t.Fatalf("expected canceled work, stats: %+v", stats)
	}
}
