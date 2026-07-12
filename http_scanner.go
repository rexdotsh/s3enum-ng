package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
)

type httpProbeFunc func(context.Context, string) (exists, selected bool, err error)

type httpScanner struct {
	context context.Context
	probe   httpProbeFunc
	queue   chan string
	jobs    sync.WaitGroup
	workers sync.WaitGroup
	stats   counters

	outputMu  sync.Mutex
	output    *bufio.Writer
	outputErr error
}

func newHTTPScanner(ctx context.Context, probe httpProbeFunc, workers int, output io.Writer) *httpScanner {
	scanner := &httpScanner{
		context: ctx,
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
		scanner.stats.queries.Add(1)
		return nil
	case <-ctx.Done():
		scanner.jobs.Done()
		return ctx.Err()
	}
}

func (scanner *httpScanner) worker() {
	defer scanner.workers.Done()
	for candidate := range scanner.queue {
		exists, selected, err := scanner.probe(scanner.context, candidate)
		if exists {
			scanner.stats.existing.Add(1)
		}
		if err != nil {
			scanner.stats.errors.Add(1)
		} else if selected {
			scanner.stats.found.Add(1)
			scanner.emit(candidate)
		}
		scanner.jobs.Done()
	}
}

func (scanner *httpScanner) finish() error {
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

func (scanner *httpScanner) emit(candidate string) {
	scanner.outputMu.Lock()
	defer scanner.outputMu.Unlock()
	if scanner.outputErr != nil {
		return
	}
	if _, err := fmt.Fprintln(scanner.output, candidate); err != nil {
		scanner.outputErr = err
		return
	}
	scanner.outputErr = scanner.output.Flush()
}

func (scanner *httpScanner) snapshot() stats {
	return stats{
		Checked:  scanner.stats.checked.Load(),
		Existing: scanner.stats.existing.Load(),
		Found:    scanner.stats.found.Load(),
		Errors:   scanner.stats.errors.Load(),
		Queries:  scanner.stats.queries.Load(),
	}
}
