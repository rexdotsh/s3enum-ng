package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

type httpProbeFunc func(context.Context, string) (exists, selected bool, err error)

type httpScanner struct {
	context context.Context
	cancel  context.CancelFunc
	probe   httpProbeFunc
	queue   chan string
	jobs    sync.WaitGroup
	workers sync.WaitGroup
	stats   counters

	outputMu  sync.Mutex
	output    *bufio.Writer
	outputErr error

	errorsMu     sync.Mutex
	errorSamples []string
}

func newHTTPScanner(ctx context.Context, probe httpProbeFunc, workers int, output io.Writer) *httpScanner {
	scannerCtx, cancel := context.WithCancel(ctx)
	scanner := &httpScanner{
		context: scannerCtx,
		cancel:  cancel,
		probe:   probe,
		queue:   make(chan string, workers*4),
		output:  bufio.NewWriterSize(output, 64*1024),
	}
	scanner.workers.Add(workers)
	for i := 0; i < workers; i++ {
		go scanner.worker()
	}
	return scanner
}

func (scanner *httpScanner) submit(ctx context.Context, candidate string) error {
	scanner.jobs.Add(1)
	select {
	case scanner.queue <- candidate:
		scanner.stats.checked.Add(1)
		return nil
	case <-scanner.context.Done():
		scanner.jobs.Done()
		return scanner.context.Err()
	case <-ctx.Done():
		scanner.jobs.Done()
		return ctx.Err()
	}
}

func (scanner *httpScanner) worker() {
	defer scanner.workers.Done()
	for candidate := range scanner.queue {
		select {
		case <-scanner.context.Done():
			scanner.stats.canceled.Add(1)
			scanner.jobs.Done()
			continue
		default:
		}

		exists, selected, err := scanner.probe(scanner.context, candidate)
		if exists {
			scanner.stats.existing.Add(1)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && scanner.context.Err() != nil {
			scanner.stats.canceled.Add(1)
		} else if err != nil {
			scanner.stats.errors.Add(1)
			scanner.recordError(candidate, err)
		} else if selected && scanner.emit(candidate) {
			scanner.stats.found.Add(1)
		}
		scanner.jobs.Done()
	}
}

func (scanner *httpScanner) finish() error {
	defer scanner.cancel()
	scanner.jobs.Wait()
	close(scanner.queue)
	scanner.workers.Wait()
	scanner.outputMu.Lock()
	defer scanner.outputMu.Unlock()
	if err := scanner.output.Flush(); scanner.outputErr == nil {
		scanner.outputErr = err
	}
	return scanner.outputErr
}

func (scanner *httpScanner) emit(candidate string) bool {
	scanner.outputMu.Lock()
	defer scanner.outputMu.Unlock()
	if scanner.outputErr != nil {
		return false
	}
	if _, err := fmt.Fprintln(scanner.output, candidate); err != nil {
		scanner.outputErr = err
		scanner.cancel()
		return false
	}
	if err := scanner.output.Flush(); err != nil {
		scanner.outputErr = err
		scanner.cancel()
		return false
	}
	return true
}

func (scanner *httpScanner) snapshot() stats {
	return stats{
		Checked:  scanner.stats.checked.Load(),
		Existing: scanner.stats.existing.Load(),
		Found:    scanner.stats.found.Load(),
		Errors:   scanner.stats.errors.Load(),
		Canceled: scanner.stats.canceled.Load(),
	}
}

func (scanner *httpScanner) recordError(candidate string, err error) {
	scanner.errorsMu.Lock()
	defer scanner.errorsMu.Unlock()
	if len(scanner.errorSamples) < maxErrorSamples {
		scanner.errorSamples = append(scanner.errorSamples, fmt.Sprintf("%q: %v", candidate, err))
	}
}

func (scanner *httpScanner) diagnostics() []string {
	scanner.errorsMu.Lock()
	defer scanner.errorsMu.Unlock()
	return append([]string(nil), scanner.errorSamples...)
}
