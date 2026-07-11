package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	for _, value := range []string{"checked: 9", "found: 1", "errors: 0", "queries/sec:"} {
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
	if got := stdout.String(); got != "v"+version+"\n" {
		t.Fatalf("version output = %q", got)
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
