# s3enum-ng

`s3enum-ng` is a high-throughput S3 bucket enumeration CLI and a drop-in
replacement for [`koenrh/s3enum`](https://github.com/koenrh/s3enum). It preserves
s3enum's candidate generation and DNS detection behavior while adding complete HTTP
existence and public-listing modes.

## Quick Start

Install the latest version with Go:

```console
go install github.com/rexdotsh/s3enum-ng@latest
```

Or run it directly without installing:

```console
go run github.com/rexdotsh/s3enum-ng@latest \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  example
```

To build the local source:

```console
go build -trimpath -ldflags="-s -w" -o s3enum-ng .
```

## Scan Modes

### DNS

DNS is the default and fastest mode. It does not make requests to S3 itself.

```console
s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  example examplecorp
```

When no resolver is supplied, s3enum-ng discovers and distributes queries across
the authoritative nameservers for `s3.amazonaws.com`. It falls back to
`/etc/resolv.conf` if discovery fails.

### HTTP Existence

Use `-http` for complete existence detection, including us-east-1:

```console
s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  -http \
  example
```

This sends an anonymous `HeadBucket` request for every candidate. Status `200`,
`301`, or `403` means the bucket exists; `404` means it does not exist.

### Public Listing

Use `-listable` to print only buckets that permit anonymous object listing:

```console
s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  -listable \
  example
```

This performs `HeadBucket` for every candidate, then `ListObjectsV2` only for
buckets that exist. A bucket can expose individual public objects while denying
listing; this mode intentionally checks listing permission only.

HTTP requests can be recorded in S3 server access logs. DNS mode retains s3enum's
DNS-only behavior.

## The us-east-1 Blind Spot

s3enum's DNS technique treats a bucket as existing when its CNAME points somewhere
other than `s3-1-w.amazonaws.com`. This works for regional buckets, but AWS returns
the same `s3-1-w.amazonaws.com` CNAME for both:

- A real bucket in us-east-1
- A bucket name that does not exist

DNS alone cannot distinguish those cases. This limitation exists in regular s3enum
as well. The `-http` and `-listable` modes fix it by checking every candidate with
S3's HTTP API instead of relying on the ambiguous CNAME.

## Candidate Generation

For each target and word, s3enum-ng generates both orderings using `-`, `_`, `.`,
and no delimiter. Every suffix is then prepended and appended using the active
delimiter. Raw targets are checked first, and duplicates are preserved to match
s3enum exactly.

Existing s3enum commands using `-wordlist`, `-suffixlist`, `-workers`, and
`-nameserver` are accepted unchanged.

## Output

Results are printed immediately, one bucket name per stdout line. The final summary
is written to stderr:

```text
checked: 100000, found: 3, errors: 0, duration: 3.5s, queries/sec: 28571
```

Listability mode reports both existing and listable counts:

```text
checked: 100000, found: 3, listable: 1, errors: 0, duration: 10.1s, queries/sec: 9901
```

## Options

```text
-resolver 1.1.1.1,8.8.8.8  DNS resolvers; repeatable (default: S3 authoritative)
-sockets 4                  UDP sockets per resolver
-concurrency 1280           maximum in-flight DNS candidates
-timeout 350ms              DNS query timeout
-retries 3                  DNS retries after timeout
-workers N                  s3enum-compatible concurrency alias
-http                       complete HTTP existence detection
-listable                   complete anonymous listing detection
-http-workers 1024          concurrent HTTP requests
-http-timeout 5s            timeout for each HTTP request
```

## Performance

This DNS-to-DNS comparison used the same target, upstream wordlist and suffix list,
and the same S3 authoritative nameserver. Both tools checked exactly 31,281
candidates with zero final errors on a 6-core Ryzen 5 3500.

| Implementation | DNS execution model | Time |
| --- | --- | ---: |
| s3enum | 50 synchronous workers | 12.02s |
| s3enum-ng | asynchronous UDP, 256 in flight across 4 sockets | 3.04s |

Under the same workload and resolver conditions, s3enum-ng completed the scan about
4x faster. Network latency and nameserver capacity still affect results, so this is
an observed comparison rather than a guaranteed multiplier.

Only scan names and infrastructure you are authorized to assess.
