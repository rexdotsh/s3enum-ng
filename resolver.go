package main

import (
	"bufio"
	"context"
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

var errRunnerStopped = errors.New("resolver stopped")

type resolverConfig struct {
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
	Queries  uint64
}

type counters struct {
	checked  atomic.Uint64
	existing atomic.Uint64
	found    atomic.Uint64
	errors   atomic.Uint64
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
	engines []*dnsEngine
	sem     chan struct{}
	wg      sync.WaitGroup
	next    atomic.Uint64
	stats   counters

	outputMu  sync.Mutex
	output    *bufio.Writer
	outputErr error
}

type dnsEngine struct {
	runner   *runner
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
	if config.concurrency > totalSockets*60000 {
		return nil, fmt.Errorf("concurrency exceeds safe DNS transaction capacity (%d)", totalSockets*60000)
	}

	r := &runner{
		sem:    make(chan struct{}, config.concurrency),
		output: bufio.NewWriterSize(output, 64*1024),
	}
	queueSize := config.concurrency/totalSockets + 64
	for _, resolver := range config.resolvers {
		address, err := resolverAddress(resolver)
		if err != nil {
			r.stopEngines()
			return nil, err
		}
		for i := 0; i < config.sockets; i++ {
			engine, err := newDNSEngine(r, address, queueSize, config.timeout, config.retries)
			if err != nil {
				r.stopEngines()
				return nil, fmt.Errorf("initialize resolver %s: %w", resolver, err)
			}
			r.engines = append(r.engines, engine)
		}
	}
	return r, nil
}

func (r *runner) submit(ctx context.Context, candidate string) error {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	req := &request{candidate: candidate}
	r.wg.Add(1)
	engine := r.engines[(r.next.Add(1)-1)%uint64(len(r.engines))]
	select {
	case engine.input <- req:
		r.stats.checked.Add(1)
		return nil
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

func (r *runner) complete(req *request, found, failed bool) {
	if !req.done.CompareAndSwap(false, true) {
		return
	}
	if failed {
		r.stats.errors.Add(1)
	}
	if found {
		r.stats.existing.Add(1)
		r.stats.found.Add(1)
		r.emit(req.candidate)
	}
	<-r.sem
	r.wg.Done()
}

func (r *runner) finish() error {
	r.wg.Wait()
	r.stopEngines()
	return r.flushOutput()
}

func (r *runner) abort() error {
	r.stopEngines()
	r.wg.Wait()
	return r.flushOutput()
}

func (r *runner) emit(candidate string) {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if r.outputErr != nil {
		return
	}
	if _, err := fmt.Fprintln(r.output, candidate); err != nil {
		r.outputErr = err
		return
	}
	r.outputErr = r.output.Flush()
}

func (r *runner) stopEngines() {
	for _, engine := range r.engines {
		engine.shutdown()
	}
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
		Queries:  r.stats.queries.Load(),
	}
}

func newDNSEngine(r *runner, address *net.UDPAddr, queueSize int, timeout time.Duration, retries int) (*dnsEngine, error) {
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
		e.runner.complete(req, false, true)
		return
	}
	message, question, err := buildQuery(req.candidate, id)
	if err != nil {
		e.remove(id, pending)
		e.runner.complete(req, false, true)
		return
	}
	if !e.setQuestion(id, pending, question) {
		return
	}

	e.runner.stats.queries.Add(1)
	written, err := e.conn.Write(message)
	if err != nil || written != len(message) {
		if e.remove(id, pending) {
			e.runner.complete(req, false, true)
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
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	select {
	case <-e.stop:
		return 0, nil, false
	default:
	}

	for i := 0; i < len(e.pending); i++ {
		id := e.nextID
		e.nextID++
		if e.pending[id] == nil {
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
			e.runner.complete(pending.request, found, responseErr != nil)
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
		if req.attempt < e.retries {
			req.attempt++
			select {
			case e.retry <- req:
			case <-e.stop:
				e.runner.complete(req, false, true)
			}
		} else {
			e.runner.complete(req, false, true)
		}
	}
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
		e.runner.complete(req, false, true)
	}
}

func (e *dnsEngine) drainQueues() {
	for {
		select {
		case req := <-e.retry:
			e.runner.complete(req, false, true)
		case req := <-e.input:
			e.runner.complete(req, false, true)
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
