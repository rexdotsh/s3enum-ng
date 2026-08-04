package main

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errRunnerStopped = errors.New("resolver stopped")
	errDNSCapacity   = errors.New("DNS transaction capacity exhausted")
)

const maxErrorSamples = 5

type resolverConfig struct {
	context     context.Context
	resolvers   []string
	sockets     int
	concurrency int
	timeout     time.Duration
	retries     int
}

type stats struct {
	Checked  uint64
	Existing uint64
	Found    uint64
	Errors   uint64
	Canceled uint64
	Queries  uint64
}

type counters struct {
	checked  atomic.Uint64
	existing atomic.Uint64
	found    atomic.Uint64
	errors   atomic.Uint64
	canceled atomic.Uint64
	queries  atomic.Uint64
}

type request struct {
	candidate string
	attempt   int
	done      atomic.Bool
}

type pendingQuery struct {
	request  *request
	question string
	deadline int64
}

type runner struct {
	context    context.Context
	cancel     context.CancelFunc
	engines    []*dnsEngine
	sem        chan struct{}
	wg         sync.WaitGroup
	next       atomic.Uint64
	stats      counters
	dispatchMu sync.RWMutex

	outputMu  sync.Mutex
	output    *bufio.Writer
	outputErr error

	errorsMu     sync.Mutex
	errorSamples []string
}

type dnsEngine struct {
	runner   *runner
	index    int
	conn     *net.UDPConn
	timeout  time.Duration
	retries  int
	input    chan *request
	retry    chan *request
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	pendingMu sync.Mutex
	pending   [1 << 16]*pendingQuery
	nextID    uint16
}

func newRunner(config resolverConfig, output io.Writer) (*runner, error) {
	if len(config.resolvers) == 0 {
		return nil, errors.New("at least one resolver is required")
	}
	if config.sockets <= 0 || config.concurrency <= 0 || config.timeout <= 0 || config.retries < 0 {
		return nil, errors.New("invalid resolver configuration")
	}
	totalSockets := len(config.resolvers) * config.sockets
	if totalSockets > maxDNSEngines {
		return nil, fmt.Errorf("DNS engine count exceeds limit (%d)", maxDNSEngines)
	}
	if config.concurrency > totalSockets*60000 {
		return nil, fmt.Errorf("concurrency exceeds safe DNS transaction capacity (%d)", totalSockets*60000)
	}

	ctx := config.context
	if ctx == nil {
		ctx = context.Background()
	}
	runnerCtx, cancel := context.WithCancel(ctx)
	r := &runner{
		context: runnerCtx,
		cancel:  cancel,
		sem:     make(chan struct{}, config.concurrency),
		output:  bufio.NewWriterSize(output, 64*1024),
	}
	queueSize := config.concurrency/totalSockets + 64
	for _, resolver := range config.resolvers {
		address, err := resolverAddress(resolver)
		if err != nil {
			r.cancel()
			r.stopEngines()
			return nil, err
		}
		for i := 0; i < config.sockets; i++ {
			engine, err := newDNSEngine(r, len(r.engines), address, queueSize, config.timeout, config.retries)
			if err != nil {
				r.cancel()
				r.stopEngines()
				return nil, fmt.Errorf("initialize resolver %s: %w", resolver, err)
			}
			r.engines = append(r.engines, engine)
		}
	}
	go func() {
		<-r.context.Done()
		r.shutdownEngines()
	}()
	return r, nil
}

func (r *runner) submit(ctx context.Context, candidate string) error {
	select {
	case r.sem <- struct{}{}:
	case <-r.context.Done():
		return errRunnerStopped
	case <-ctx.Done():
		return ctx.Err()
	}

	req := &request{candidate: candidate}
	r.wg.Add(1)
	engine := r.engines[(r.next.Add(1)-1)%uint64(len(r.engines))]
	r.dispatchMu.RLock()
	defer r.dispatchMu.RUnlock()
	select {
	case engine.input <- req:
		r.stats.checked.Add(1)
		return nil
	case <-r.context.Done():
		<-r.sem
		r.wg.Done()
		return errRunnerStopped
	case <-ctx.Done():
		<-r.sem
		r.wg.Done()
		return ctx.Err()
	case <-engine.stop:
		<-r.sem
		r.wg.Done()
		return errRunnerStopped
	}
}

func (r *runner) complete(req *request, found bool, queryErr error, canceled bool) {
	if !req.done.CompareAndSwap(false, true) {
		return
	}
	if canceled {
		r.stats.canceled.Add(1)
	} else if queryErr != nil {
		r.stats.errors.Add(1)
		r.recordError(req.candidate, queryErr)
	}
	if found {
		r.stats.existing.Add(1)
		if r.emit(req.candidate) {
			r.stats.found.Add(1)
		}
	}
	<-r.sem
	r.wg.Done()
}

func (r *runner) finish() error {
	r.wg.Wait()
	r.cancel()
	r.stopEngines()
	return r.flushOutput()
}

func (r *runner) abort() error {
	r.cancel()
	r.stopEngines()
	r.wg.Wait()
	return r.flushOutput()
}

func (r *runner) emit(candidate string) bool {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if r.outputErr != nil {
		return false
	}
	if _, err := fmt.Fprintln(r.output, candidate); err != nil {
		r.outputErr = err
		r.cancelScan()
		return false
	}
	if err := r.output.Flush(); err != nil {
		r.outputErr = err
		r.cancelScan()
		return false
	}
	return true
}

func (r *runner) cancelScan() {
	r.cancel()
}

func (r *runner) shutdownEngines() {
	r.dispatchMu.Lock()
	defer r.dispatchMu.Unlock()
	for _, engine := range r.engines {
		engine.shutdown()
	}
}

func (r *runner) stopEngines() {
	r.shutdownEngines()
	for _, engine := range r.engines {
		engine.wg.Wait()
	}
}

func (r *runner) flushOutput() error {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if err := r.output.Flush(); r.outputErr == nil {
		r.outputErr = err
	}
	return r.outputErr
}

func (r *runner) snapshot() stats {
	return stats{
		Checked:  r.stats.checked.Load(),
		Existing: r.stats.existing.Load(),
		Found:    r.stats.found.Load(),
		Errors:   r.stats.errors.Load(),
		Canceled: r.stats.canceled.Load(),
		Queries:  r.stats.queries.Load(),
	}
}

func (r *runner) recordError(candidate string, err error) {
	r.errorsMu.Lock()
	defer r.errorsMu.Unlock()
	if len(r.errorSamples) < maxErrorSamples {
		r.errorSamples = append(r.errorSamples, fmt.Sprintf("%q: %v", candidate, err))
	}
}

func (r *runner) diagnostics() []string {
	r.errorsMu.Lock()
	defer r.errorsMu.Unlock()
	return append([]string(nil), r.errorSamples...)
}

func newDNSEngine(r *runner, index int, address *net.UDPAddr, queueSize int, timeout time.Duration, retries int) (*dnsEngine, error) {
	network := "udp4"
	if address.IP.To4() == nil {
		network = "udp6"
	}
	conn, err := net.DialUDP(network, nil, address)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)

	engine := &dnsEngine{
		runner:  r,
		index:   index,
		conn:    conn,
		timeout: timeout,
		retries: retries,
		input:   make(chan *request, queueSize),
		retry:   make(chan *request, queueSize),
		stop:    make(chan struct{}),
	}
	engine.wg.Add(3)
	go engine.sendLoop()
	go engine.receiveLoop()
	go engine.timeoutLoop()
	return engine, nil
}

func (e *dnsEngine) sendLoop() {
	defer e.wg.Done()
	defer e.drainQueues()
	for {
		var req *request
		select {
		case <-e.stop:
			return
		case req = <-e.retry:
		default:
			select {
			case <-e.stop:
				return
			case req = <-e.retry:
			case req = <-e.input:
			}
		}
		e.send(req)
	}
}

func (e *dnsEngine) send(req *request) {
	id, pending, ok := e.reserve(req)
	if !ok {
		if e.runner.context.Err() != nil {
			e.runner.complete(req, false, nil, true)
		} else {
			e.runner.complete(req, false, errDNSCapacity, false)
		}
		return
	}
	message, question, err := buildQuery(req.candidate, id)
	if err != nil {
		e.remove(id, pending)
		e.runner.complete(req, false, err, false)
		return
	}
	if !e.setQuestion(id, pending, question) {
		return
	}

	e.runner.stats.queries.Add(1)
	written, err := e.conn.Write(message)
	if err != nil || written != len(message) {
		if e.remove(id, pending) {
			if err == nil {
				err = io.ErrShortWrite
			}
			e.runner.retryRequest(e, req, fmt.Errorf("send DNS query: %w", err))
		}
	}
}

func (e *dnsEngine) setQuestion(id uint16, expected *pendingQuery, question string) bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if e.pending[id] != expected {
		return false
	}
	expected.question = question
	return true
}

func (e *dnsEngine) reserve(req *request) (uint16, *pendingQuery, bool) {
	var random [2]byte
	_, randomErr := cryptorand.Read(random[:])
	start := e.nextID
	if randomErr == nil {
		start = binary.BigEndian.Uint16(random[:])
	}

	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	select {
	case <-e.stop:
		return 0, nil, false
	default:
	}

	for i := 0; i < len(e.pending); i++ {
		id := start + uint16(i)
		if e.pending[id] == nil {
			e.nextID = id + 1
			pending := &pendingQuery{
				request:  req,
				deadline: time.Now().Add(e.timeout).UnixNano(),
			}
			e.pending[id] = pending
			return id, pending, true
		}
	}
	return 0, nil, false
}

func (e *dnsEngine) receiveLoop() {
	defer e.wg.Done()
	buffer := make([]byte, maxDNSMessageBytes)
	for {
		count, err := e.conn.Read(buffer)
		if err != nil {
			select {
			case <-e.stop:
				return
			default:
				continue
			}
		}
		id, question, found, responseErr := parseResponse(buffer[:count])
		if question == "" {
			continue
		}

		e.pendingMu.Lock()
		pending := e.pending[id]
		if pending != nil && strings.EqualFold(pending.question, question) {
			e.pending[id] = nil
		} else {
			pending = nil
		}
		e.pendingMu.Unlock()
		if pending != nil {
			if responseErr != nil {
				e.runner.retryRequest(e, pending.request, responseErr)
			} else {
				e.runner.complete(pending.request, found, nil, false)
			}
		}
	}
}

func (e *dnsEngine) timeoutLoop() {
	defer e.wg.Done()
	interval := e.timeout / 4
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stop:
			return
		case now := <-ticker.C:
			e.expire(now.UnixNano())
		}
	}
}

func (e *dnsEngine) expire(now int64) {
	var expired []*request
	e.pendingMu.Lock()
	for id, pending := range e.pending {
		if pending != nil && pending.deadline <= now {
			e.pending[id] = nil
			expired = append(expired, pending.request)
		}
	}
	e.pendingMu.Unlock()

	for _, req := range expired {
		e.runner.retryRequest(e, req, errors.New("DNS query timed out"))
	}
}

func (r *runner) retryRequest(from *dnsEngine, req *request, cause error) {
	if r.context.Err() != nil {
		r.complete(req, false, nil, true)
		return
	}
	if req.attempt >= from.retries {
		r.complete(req, false, fmt.Errorf("%w after %d attempt(s)", cause, req.attempt+1), false)
		return
	}
	req.attempt++

	r.dispatchMu.RLock()
	defer r.dispatchMu.RUnlock()
	start := (from.index + req.attempt) % len(r.engines)
	for offset := 0; offset < len(r.engines); offset++ {
		target := r.engines[(start+offset)%len(r.engines)]
		if len(r.engines) > 1 && target == from {
			continue
		}
		select {
		case target.retry <- req:
			return
		case <-target.stop:
			continue
		case <-r.context.Done():
			r.complete(req, false, nil, true)
			return
		}
	}
	r.complete(req, false, errRunnerStopped, r.context.Err() != nil)
}

func (e *dnsEngine) remove(id uint16, expected *pendingQuery) bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if e.pending[id] != expected {
		return false
	}
	e.pending[id] = nil
	return true
}

func (e *dnsEngine) shutdown() {
	e.stopOnce.Do(func() {
		close(e.stop)
		_ = e.conn.Close()
		e.failPending()
	})
}

func (e *dnsEngine) failPending() {
	var requests []*request
	e.pendingMu.Lock()
	for id, pending := range e.pending {
		if pending != nil {
			requests = append(requests, pending.request)
			e.pending[id] = nil
		}
	}
	e.pendingMu.Unlock()
	for _, req := range requests {
		e.runner.complete(req, false, nil, true)
	}
}

func (e *dnsEngine) drainQueues() {
	for {
		select {
		case req := <-e.retry:
			e.runner.complete(req, false, nil, true)
		case req := <-e.input:
			e.runner.complete(req, false, nil, true)
		default:
			return
		}
	}
}

func loadSystemResolvers(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read resolver configuration: %w", err)
	}
	defer file.Close()

	var resolvers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "nameserver" {
			resolvers = append(resolvers, fields[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read resolver configuration: %w", err)
	}
	if len(resolvers) == 0 {
		return nil, errors.New("no nameservers found in resolver configuration")
	}
	return resolvers, nil
}

func discoverAuthoritativeResolvers(ctx context.Context, domain string) ([]string, error) {
	nameservers, err := net.DefaultResolver.LookupNS(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("discover authoritative nameservers: %w", err)
	}

	seen := make(map[string]struct{})
	var resolvers []string
	for _, nameserver := range nameservers {
		host := strings.TrimSuffix(nameserver.Host, ".")
		addresses, lookupErr := net.DefaultResolver.LookupIP(ctx, "ip4", host)
		if lookupErr != nil {
			continue
		}
		for _, address := range addresses {
			value := address.String()
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			resolvers = append(resolvers, value)
		}
	}
	if len(resolvers) == 0 {
		return nil, errors.New("authoritative nameservers have no IPv4 addresses")
	}
	return resolvers, nil
}

func resolverAddress(value string) (*net.UDPAddr, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("resolver address is empty")
	}
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
	}
	if net.ParseIP(value) != nil {
		value = net.JoinHostPort(value, "53")
	} else if _, _, err := net.SplitHostPort(value); err != nil {
		value = net.JoinHostPort(value, "53")
	}
	address, err := net.ResolveUDPAddr("udp", value)
	if err != nil {
		return nil, fmt.Errorf("resolve nameserver %q: %w", value, err)
	}
	return address, nil
}
