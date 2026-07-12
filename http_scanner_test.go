package main

import (
	"bytes"
	"context"
	"errors"
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
	if stats.Checked != 4 || stats.Existing != 3 || stats.Found != 1 || stats.Errors != 1 || stats.Queries != 4 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
