# s3enum-ng

`s3enum-ng` is a high-throughput S3 bucket enumeration CLI and an
s3enum-compatible successor to
[`koenrh/s3enum`](https://github.com/koenrh/s3enum). It preserves s3enum's
candidate generation and DNS detection behavior while adding hardened
asynchronous DNS, HTTP existence evidence, and public-listing modes.

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

The repository includes starter lists from upstream s3enum. After cloning, run:

```console
go run . \
  -wordlist examples/wordlist.txt \
  -suffixlist examples/suffixlist.txt \
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

When no resolver is supplied, s3enum-ng discovers and distributes queries
across the authoritative nameservers for `s3.amazonaws.com`. It falls back to
`/etc/resolv.conf` if discovery fails. Timeouts and transient DNS responses are
retried through another available resolver engine.

### HTTP Existence

Use `-http` for stronger existence detection, including evidence for buckets in
us-east-1:

```console
s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  -http \
  example
```

This sends an anonymous `HeadBucket` request for every candidate. Status `200`,
an S3 region redirect, or an `x-amz-bucket-region` response header is treated as
positive evidence. A `404` without a region header is treated as absent. A
`400` or `403` without a region header is inconclusive and counted as an error.

AWS documents that absent and unauthorized buckets can return generic `400`,
`403`, or `404` responses without a body. Anonymous HTTP enumeration therefore
cannot guarantee a definitive answer for every bucket policy or network path.

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
confirmed hits. A bucket can expose individual public objects while denying
listing; this mode intentionally checks listing permission only.

HTTP requests can be recorded in S3 server access logs. The HTTP modes honor
`HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY`. DNS mode retains s3enum's DNS-only
behavior, although candidate names remain visible to DNS infrastructure and
network observers.

The HTTP modes target general-purpose buckets in the standard AWS partition.
Directory buckets, access points, S3 on Outposts, and partition-specific
endpoints are outside the current scope.

## The us-east-1 Blind Spot

s3enum's DNS technique treats a bucket as existing when its CNAME points
somewhere other than `s3-1-w.amazonaws.com`. This works for regional buckets,
but AWS returns the same `s3-1-w.amazonaws.com` CNAME for both:

- A real bucket in us-east-1
- A bucket name that does not exist

DNS alone cannot distinguish those cases. This limitation exists in regular
s3enum as well. The `-http` and `-listable` modes check S3's HTTP API instead of
relying only on the ambiguous CNAME, subject to the anonymous-response caveat
described above.

## Candidate Generation

For each target and word, s3enum-ng generates both orderings using `-`, `_`,
`.`, and no delimiter. Every suffix is then prepended and appended using the
active delimiter. Raw targets are checked first, and duplicates are preserved
to match s3enum exactly.

Existing s3enum commands using `-wordlist`, `-suffixlist`, `-workers`, and
`-nameserver` are accepted unchanged. Word and suffix entries are consumed
verbatim, and all required files are opened before network scanning begins.

The project source is available under the MIT License. The files under
`examples/` come from upstream s3enum and retain the ISC license in
`examples/LICENSE`.

## Output

Results are printed immediately, one bucket name per stdout line. The final DNS
summary is written to stderr:

```text
checked: 100000, found: 3, errors: 0, canceled: 0, packets: 100012, duration: 3.5s, packets/sec: 28575
```

HTTP existence mode reports actual requests, including retries:

```text
checked: 100000, found: 3, errors: 0, canceled: 0, requests: 100005, duration: 10.1s, requests/sec: 9901
```

Listability mode reports both existing and listable counts:

```text
checked: 100000, existing: 3, listable: 1, errors: 0, canceled: 0, requests: 100008, duration: 12.1s, requests/sec: 8265
```

Up to five representative probe errors are printed after the summary. Exit
code `0` means the scan completed without probe or output errors, `1` means the
scan was incomplete or another operational error occurred, `2` means invalid
usage, and `130` means the process was interrupted.

## Options

```text
s3enum-ng -wordlist FILE -suffixlist FILE [options] <name>...
```

| Option | Description | Default |
| --- | --- | --- |
| `-wordlist FILE` | Word list used to generate candidates | required |
| `-suffixlist FILE` | Suffix list used to generate candidates | required |
| `-resolver HOST[:PORT]` | DNS resolver; repeatable or comma-separated (`-nameserver` alias) | S3 authoritative |
| `-sockets N` | UDP sockets per resolver; maximum 64 | `4` |
| `-concurrency N` | Maximum in-flight DNS candidates | `1280` |
| `-timeout DURATION` | Timeout for each DNS attempt | `350ms` |
| `-retries N` | Retries after timeouts or transient responses | `3` |
| `-http` | Use HTTP existence evidence instead of DNS | disabled |
| `-listable` | Print anonymously listable buckets (`-check-public` alias) | disabled |
| `-http-workers N` | Concurrent HTTP checks; maximum 4096 | `1024` |
| `-http-timeout DURATION` | Timeout for each HTTP request | `5s` |
| `-workers N` | Compatibility alias for concurrency | unset |
| `-version` | Print the build version and exit | disabled |

At least one target is required, and `-http` cannot be combined with
`-listable`. Total DNS resolver/socket engines are limited to 256. In an HTTP
mode, `-workers` also sets HTTP workers unless `-http-workers` is provided.
Durations accept values such as `250ms`, `2s`, or `1m`. Run `s3enum-ng -h` for
built-in help.

## Performance

This DNS-to-DNS comparison used the same target, upstream wordlist and suffix
list, and the same S3 authoritative nameserver. Both tools checked exactly
31,281 candidates with zero final errors on a 6-core Ryzen 5 3500.

| Implementation | DNS execution model | Time |
| --- | --- | ---: |
| s3enum | 50 synchronous workers | 12.02s |
| s3enum-ng | asynchronous UDP, 256 in flight across 4 sockets | 3.04s |

Under the same workload and resolver conditions, s3enum-ng completed the scan
about 4x faster. Network latency and nameserver capacity still affect results,
so this is an observed comparison rather than a guaranteed multiplier.

Only scan names and infrastructure you are authorized to assess.
