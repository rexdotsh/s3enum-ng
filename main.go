package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var version = "0.3.0"

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
	httpWorkers := flags.Int("http-workers", 1024, "concurrent S3 HTTP checks")
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
		if strings.HasPrefix(version, "v") {
			fmt.Fprintln(stdout, version)
		} else {
			fmt.Fprintln(stdout, "v"+version)
		}
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
	if *concurrency <= 0 || *sockets <= 0 || *timeout <= 0 || *retries < 0 || *httpWorkers <= 0 || *httpTimeout <= 0 {
		fmt.Fprintln(stderr, "concurrency, sockets, timeouts, and HTTP workers must be positive; retries cannot be negative")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	suffixes, err := readLines(*suffixlist)
	if err != nil {
		fmt.Fprintf(stderr, "read suffix list: %v\n", err)
		return 1
	}
	var submit submitFunc
	var finish func() error
	var abort func() error
	var snapshot func() stats
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
		finish = httpScanner.finish
		abort = httpScanner.finish
		snapshot = httpScanner.snapshot
	} else {
		if len(resolvers) == 0 {
			resolvers, err = discoverAuthoritativeResolvers(ctx, strings.TrimSuffix(s3Suffix, "."))
			if err != nil {
				resolvers, err = loadSystemResolvers("/etc/resolv.conf")
				if err != nil {
					fmt.Fprintln(stderr, err)
					return 1
				}
			}
		}

		runner, err := newRunner(resolverConfig{
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
	}

	start := time.Now()
	produceErr := produceCandidates(ctx, flags.Args(), *wordlist, suffixes, submit)

	var outputErr error
	if ctx.Err() != nil {
		outputErr = abort()
	} else {
		outputErr = finish()
	}
	duration := time.Since(start)
	result := snapshot()
	qps := float64(result.Queries) / duration.Seconds()
	if listable {
		fmt.Fprintf(stderr, "checked: %d, found: %d, listable: %d, errors: %d, duration: %s, queries/sec: %.0f\n",
			result.Checked, result.Existing, result.Found, result.Errors, duration.Round(time.Millisecond), qps)
	} else {
		fmt.Fprintf(stderr, "checked: %d, found: %d, errors: %d, duration: %s, queries/sec: %.0f\n",
			result.Checked, result.Found, result.Errors, duration.Round(time.Millisecond), qps)
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
	return 0
}
