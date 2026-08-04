package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

const (
	defaultHTTPWorkers = 1024
	maxHTTPWorkers     = 4096
	maxDNSSockets      = 64
	maxDNSEngines      = 256
)

var version = "devel"

type stringListFlag []string

func (values *stringListFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *stringListFlag) Set(value string) error {
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			*values = append(*values, entry)
		}
	}
	return nil
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("s3enum-ng", flag.ContinueOnError)
	flags.SetOutput(stderr)
	wordlist := flags.String("wordlist", "", "path to word list")
	suffixlist := flags.String("suffixlist", "", "path to suffix list")
	concurrency := flags.Int("concurrency", 1280, "maximum in-flight candidates")
	workers := flags.Int("workers", 0, "compatibility alias for -concurrency")
	sockets := flags.Int("sockets", 4, "UDP sockets per resolver")
	timeout := flags.Duration("timeout", 350*time.Millisecond, "DNS query timeout")
	retries := flags.Int("retries", 3, "retries after a timeout")
	httpMode := flags.Bool("http", false, "use HTTP HeadBucket instead of DNS")
	listable := false
	flags.BoolVar(&listable, "listable", false, "only print anonymously listable buckets (uses HTTP)")
	flags.BoolVar(&listable, "check-public", false, "alias for -listable")
	httpWorkers := flags.Int("http-workers", defaultHTTPWorkers, "concurrent S3 HTTP checks")
	httpTimeout := flags.Duration("http-timeout", 5*time.Second, "S3 HTTP request timeout")
	showVersion := flags.Bool("version", false, "print version")
	var resolvers stringListFlag
	flags.Var(&resolvers, "resolver", "DNS resolver host[:port], repeatable or comma-separated")
	flags.Var(&resolvers, "nameserver", "alias for -resolver")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: s3enum-ng -wordlist FILE -suffixlist FILE [options] <name>...")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	workersSet := false
	httpWorkersSet := false
	flags.Visit(func(current *flag.Flag) {
		switch current.Name {
		case "workers":
			workersSet = true
		case "http-workers":
			httpWorkersSet = true
		}
	})
	if *showVersion {
		fmt.Fprintln(stdout, currentVersion())
		return 0
	}
	if workersSet {
		if *workers <= 0 {
			fmt.Fprintln(stderr, "workers must be a positive number")
			return 2
		}
		*concurrency = *workers
		if (*httpMode || listable) && !httpWorkersSet {
			*httpWorkers = *workers
		}
	}
	if *wordlist == "" || *suffixlist == "" || flags.NArg() == 0 {
		flags.Usage()
		return 2
	}
	if *httpMode && listable {
		fmt.Fprintln(stderr, "-http and -listable select different output modes and cannot be combined")
		return 2
	}
	if *httpMode || listable {
		if *httpWorkers <= 0 || *httpWorkers > maxHTTPWorkers || *httpTimeout <= 0 {
			fmt.Fprintf(stderr, "HTTP workers must be between 1 and %d and HTTP timeout must be positive\n", maxHTTPWorkers)
			return 2
		}
	} else {
		if *concurrency <= 0 || *sockets <= 0 || *sockets > maxDNSSockets || *timeout <= 0 || *retries < 0 || len(resolvers)*(*sockets) > maxDNSEngines {
			fmt.Fprintf(stderr, "concurrency and DNS timeout must be positive, sockets must be between 1 and %d, total DNS engines cannot exceed %d, and retries cannot be negative\n", maxDNSSockets, maxDNSEngines)
			return 2
		}
	}

	suffixes, err := readLines(*suffixlist)
	if err != nil {
		fmt.Fprintf(stderr, "read suffix list: %v\n", err)
		return 1
	}
	words, err := os.Open(*wordlist)
	if err != nil {
		fmt.Fprintf(stderr, "open word list: %v\n", err)
		return 1
	}
	defer words.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var submit submitFunc
	var finish func() error
	var abort func() error
	var snapshot func() stats
	var diagnostics func() []string
	if *httpMode || listable {
		checker := newS3ListChecker(*httpTimeout, *httpWorkers)
		var probe httpProbeFunc
		if *httpMode {
			probe = func(ctx context.Context, candidate string) (bool, bool, error) {
				exists, err := checker.ProbeExists(ctx, candidate)
				return exists, exists, err
			}
			fmt.Fprintln(stderr, "warning: -http sends logged S3 HeadBucket requests for every candidate")
		} else {
			probe = func(ctx context.Context, candidate string) (bool, bool, error) {
				exists, err := checker.ProbeExists(ctx, candidate)
				if err != nil || !exists {
					return exists, false, err
				}
				_, listable, err := checker.ProbeListing(ctx, candidate)
				return true, listable, err
			}
			fmt.Fprintln(stderr, "warning: -listable sends logged HeadBucket requests for every candidate and ListObjectsV2 for hits")
		}
		httpScanner := newHTTPScanner(ctx, probe, *httpWorkers, stdout)
		submit = httpScanner.submit
		finish = func() error {
			defer checker.Close()
			return httpScanner.finish()
		}
		abort = finish
		snapshot = func() stats {
			result := httpScanner.snapshot()
			result.Queries = checker.Requests()
			return result
		}
		diagnostics = httpScanner.diagnostics
	} else {
		if len(resolvers) == 0 {
			resolvers, err = discoverAuthoritativeResolvers(ctx, strings.TrimSuffix(s3Suffix, "."))
			if err != nil {
				fmt.Fprintf(stderr, "warning: authoritative resolver discovery failed (%v); using system resolvers\n", err)
				resolvers, err = loadSystemResolvers("/etc/resolv.conf")
				if err != nil {
					fmt.Fprintln(stderr, err)
					return 1
				}
			}
		}

		runner, err := newRunner(resolverConfig{
			context:     ctx,
			resolvers:   resolvers,
			sockets:     *sockets,
			concurrency: *concurrency,
			timeout:     *timeout,
			retries:     *retries,
		}, stdout)
		if err != nil {
			fmt.Fprintf(stderr, "initialize DNS engine: %v\n", err)
			return 1
		}
		submit = runner.submit
		finish = runner.finish
		abort = runner.abort
		snapshot = runner.snapshot
		diagnostics = runner.diagnostics
	}

	start := time.Now()
	produceErr := produceCandidates(ctx, flags.Args(), words, suffixes, submit)

	var outputErr error
	if ctx.Err() != nil {
		outputErr = abort()
	} else {
		outputErr = finish()
	}
	duration := time.Since(start)
	result := snapshot()
	rate := float64(result.Queries) / max(duration.Seconds(), 0.001)
	if listable {
		fmt.Fprintf(stderr, "checked: %d, existing: %d, listable: %d, errors: %d, canceled: %d, requests: %d, duration: %s, requests/sec: %.0f\n",
			result.Checked, result.Existing, result.Found, result.Errors, result.Canceled, result.Queries, duration.Round(time.Millisecond), rate)
	} else if *httpMode {
		fmt.Fprintf(stderr, "checked: %d, found: %d, errors: %d, canceled: %d, requests: %d, duration: %s, requests/sec: %.0f\n",
			result.Checked, result.Found, result.Errors, result.Canceled, result.Queries, duration.Round(time.Millisecond), rate)
	} else {
		fmt.Fprintf(stderr, "checked: %d, found: %d, errors: %d, canceled: %d, packets: %d, duration: %s, packets/sec: %.0f\n",
			result.Checked, result.Found, result.Errors, result.Canceled, result.Queries, duration.Round(time.Millisecond), rate)
	}
	for _, diagnostic := range diagnostics() {
		fmt.Fprintf(stderr, "error: %s\n", diagnostic)
	}

	if outputErr != nil {
		fmt.Fprintf(stderr, "write output: %v\n", outputErr)
		return 1
	}
	if produceErr != nil && !errors.Is(produceErr, context.Canceled) {
		fmt.Fprintln(stderr, produceErr)
		return 1
	}
	if ctx.Err() != nil {
		return 130
	}
	if result.Errors > 0 {
		return 1
	}
	return 0
}

func currentVersion() string {
	value := version
	if value == "devel" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			value = info.Main.Version
		}
	}
	if value == "devel" || strings.HasPrefix(value, "v") {
		return value
	}
	return "v" + value
}
