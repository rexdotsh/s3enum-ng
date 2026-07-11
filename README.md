# s3enum-ng

`s3enum-ng` is a high-throughput S3 bucket enumeration CLI. By default it uses DNS
CNAME queries instead of S3 API or HTTP requests and preserves the
candidate-generation behavior of
[`koenrh/s3enum`](https://github.com/koenrh/s3enum).

For every target and word, it generates both orderings with `-`, `_`, `.`, and no
delimiter. Every suffix is then prepended and appended with the same active
delimiter. Raw target names are checked first. Candidates are streamed and are not
deduplicated, matching s3enum.

## Build

Go is the only dependency.

```console
go build -trimpath -ldflags="-s -w" -o s3enum-ng .
```

## Usage

```console
./s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  example examplecorp
```

Existing buckets are printed one per line on stdout. The final summary is printed
on stderr:

```text
checked: 100000, found: 3, errors: 0, duration: 1.234s, queries/sec: 81037
```

To print only buckets that allow anonymous object listing:

```console
./s3enum-ng \
  -wordlist words.txt \
  -suffixlist suffixes.txt \
  -listable \
  example
```

This sends anonymous `ListObjectsV2` HTTP requests for DNS hits. Unlike DNS-only
enumeration, these requests can be recorded in S3 server access logs. A bucket can
also expose individual public objects while denying listing; `-listable` intentionally
does not attempt to discover that separate condition.

In this mode stdout contains only listable bucket names, while stderr distinguishes
DNS hits from confirmed listings:

```text
checked: 100000, found: 3, listable: 1, errors: 0, duration: 2.345s, queries/sec: 81037
```

Useful tuning options:

```text
-resolver 1.1.1.1,8.8.8.8  DNS resolvers; repeatable (default: S3 authoritative)
-sockets 4                  UDP sockets per resolver
-concurrency 1280           maximum in-flight candidates
-timeout 350ms              query timeout
-retries 3                  retries after timeout
-workers N                  s3enum-compatible alias for -concurrency
-listable                   only print anonymously listable buckets
-http-workers 32            concurrent listability requests
-http-timeout 5s            timeout for each listability request
```

When no `-resolver` is supplied, s3enum-ng discovers all authoritative nameservers
for `s3.amazonaws.com` and queries their IPv4 addresses directly. It falls back to
`/etc/resolv.conf` if discovery fails. This avoids recursive-resolver throttling and
distributes queries across S3's DNS infrastructure.

Public recursive resolvers commonly rate-limit large bursts. If you explicitly use
one, lower `-concurrency`; when using resolvers you control, raise it for high-latency
links. Retries reduce false negatives on lossy networks but increase tail latency and
total DNS traffic.

Existing s3enum command lines using `-wordlist`, `-suffixlist`, `-workers`, and
`-nameserver` are accepted unchanged. Bucket output remains one name per stdout
line; only the stderr statistics format differs.

## Detection

Each candidate queries CNAME for `<candidate>.s3.amazonaws.com.`. A candidate is
reported when a CNAME answer points anywhere other than
`s3-1-w.amazonaws.com.`. This intentionally has the same us-east-1 limitation as
s3enum.

Only scan names and infrastructure you are authorized to assess.

## Performance

The upstream example lists contain 230 words and 8 suffixes, producing 31,281
candidates for one target. On a 6-core Ryzen 5 3500 host, using the same target and
lists:

| Command | Wall time | Final errors |
| --- | ---: | ---: |
| s3enum, default system resolver | 472.08s | 12,244 |
| s3enum, one S3 authoritative resolver | 14.21s | 0 |
| s3enum-ng, default authoritative discovery | 3.47s | 0 |

That run was 136x faster default-to-default. Resolver capacity and network latency
dominate real scans, so results vary by host and network; this is an observed result,
not a guaranteed multiplier.
