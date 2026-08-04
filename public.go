package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	maxListResponseBytes = 64 * 1024
	httpAttempts         = 3
)

type s3ListChecker struct {
	client   *http.Client
	endpoint string
	requests atomic.Uint64
}

type listingProbe struct {
	exists   bool
	listable bool
	region   string
	redirect bool
}

type retryableHTTPError struct {
	cause      error
	retryAfter time.Duration
}

func (err *retryableHTTPError) Error() string { return err.cause.Error() }
func (err *retryableHTTPError) Unwrap() error { return err.cause }

func newS3ListChecker(timeout time.Duration, workers int) *s3ListChecker {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          workers * 2,
		MaxIdleConnsPerHost:   workers,
		MaxConnsPerHost:       workers,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		DisableCompression:    true,
		ForceAttemptHTTP2:     true,
	}
	return &s3ListChecker{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint: "https://s3.amazonaws.com",
	}
}

func (checker *s3ListChecker) IsListable(ctx context.Context, bucket string) (bool, error) {
	_, listable, err := checker.ProbeListing(ctx, bucket)
	return listable, err
}

func (checker *s3ListChecker) Close() {
	checker.client.CloseIdleConnections()
}

func (checker *s3ListChecker) Requests() uint64 {
	return checker.requests.Load()
}

func (checker *s3ListChecker) ProbeExists(ctx context.Context, bucket string) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < httpAttempts; attempt++ {
		exists, err := checker.headBucket(ctx, bucket)
		if err == nil {
			return exists, nil
		}
		lastErr = err
		if attempt == httpAttempts-1 || !isRetryableHTTPError(err) {
			break
		}
		if err := waitForRetry(ctx, attempt, err); err != nil {
			return false, err
		}
	}
	return false, lastErr
}

func (checker *s3ListChecker) headBucket(ctx context.Context, bucket string) (bool, error) {
	target, err := bucketURL(checker.endpoint, bucket, false)
	if err != nil {
		return false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if err != nil {
		return false, err
	}
	request.Header.Set("User-Agent", "s3enum-ng/"+currentVersion())
	checker.requests.Add(1)
	response, err := checker.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, &retryableHTTPError{cause: err}
	}
	defer response.Body.Close()

	region := response.Header.Get("x-amz-bucket-region")
	switch response.StatusCode {
	case http.StatusOK, http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect, http.StatusForbidden:
		if response.StatusCode != http.StatusForbidden || region != "" {
			return true, nil
		}
		return false, fmt.Errorf("S3 HeadBucket returned HTTP %d without a bucket region; existence is inconclusive", response.StatusCode)
	case http.StatusBadRequest, http.StatusNotFound:
		if region != "" {
			return true, nil
		}
		if response.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, fmt.Errorf("S3 HeadBucket returned HTTP %d without a bucket region; existence is inconclusive", response.StatusCode)
	default:
		return false, classifyHTTPStatus("S3 HeadBucket", response)
	}
}

func (checker *s3ListChecker) ProbeListing(ctx context.Context, bucket string) (bool, bool, error) {
	result, err := checker.probeListingWithRetry(ctx, checker.endpoint, bucket)
	if err != nil || !result.redirect {
		return result.exists, result.listable, err
	}
	if result.region == "" {
		return true, false, errors.New("S3 redirect did not include a bucket region")
	}
	result, err = checker.probeListingWithRetry(ctx, "https://s3."+result.region+".amazonaws.com", bucket)
	if err != nil {
		return true, false, err
	}
	return true, result.listable, nil
}

func (checker *s3ListChecker) probeListingWithRetry(ctx context.Context, endpoint, bucket string) (listingProbe, error) {
	var lastErr error
	for attempt := 0; attempt < httpAttempts; attempt++ {
		result, err := checker.probeListing(ctx, endpoint, bucket)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt == httpAttempts-1 || !isRetryableHTTPError(err) {
			break
		}
		if err := waitForRetry(ctx, attempt, err); err != nil {
			return listingProbe{}, err
		}
	}
	return listingProbe{}, lastErr
}

func (checker *s3ListChecker) probeListing(ctx context.Context, endpoint, bucket string) (listingProbe, error) {
	target, err := bucketURL(endpoint, bucket, true)
	if err != nil {
		return listingProbe{}, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return listingProbe{}, err
	}
	request.Header.Set("User-Agent", "s3enum-ng/"+currentVersion())
	checker.requests.Add(1)
	response, err := checker.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return listingProbe{}, ctx.Err()
		}
		return listingProbe{}, &retryableHTTPError{cause: err}
	}
	defer response.Body.Close()

	region := response.Header.Get("x-amz-bucket-region")
	switch response.StatusCode {
	case http.StatusOK:
		root, err := xmlRoot(io.LimitReader(response.Body, maxListResponseBytes))
		if err != nil {
			return listingProbe{}, err
		}
		if root.Local != "ListBucketResult" {
			return listingProbe{}, fmt.Errorf("unexpected S3 list response root %q", root.Local)
		}
		return listingProbe{exists: true, listable: true, region: region}, nil
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return listingProbe{exists: true, region: region, redirect: true}, nil
	case http.StatusForbidden:
		if region == "" {
			return listingProbe{}, fmt.Errorf("S3 list request returned HTTP %d without a bucket region; existence is inconclusive", response.StatusCode)
		}
		return listingProbe{exists: true, region: region}, nil
	case http.StatusBadRequest, http.StatusNotFound:
		if region != "" {
			return listingProbe{exists: true, region: region}, nil
		}
		if response.StatusCode == http.StatusNotFound {
			return listingProbe{}, nil
		}
		return listingProbe{}, fmt.Errorf("S3 list request returned HTTP %d without a bucket region; existence is inconclusive", response.StatusCode)
	default:
		return listingProbe{}, classifyHTTPStatus("S3 list request", response)
	}
}

func bucketURL(endpoint, bucket string, listing bool) (string, error) {
	if err := validateBucketCandidate(bucket); err != nil {
		return "", err
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	target.Path = strings.TrimSuffix(target.Path, "/") + "/" + bucket
	if listing {
		query := target.Query()
		query.Set("list-type", "2")
		query.Set("max-keys", "1")
		target.RawQuery = query.Encode()
	}
	return target.String(), nil
}

func validateBucketCandidate(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return fmt.Errorf("invalid bucket name %q: length must be between 3 and 63 bytes", bucket)
	}
	for index := 0; index < len(bucket); index++ {
		character := bucket[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("invalid bucket name %q: unsupported character", bucket)
	}
	if !isASCIIAlphaNumeric(bucket[0]) || !isASCIIAlphaNumeric(bucket[len(bucket)-1]) {
		return fmt.Errorf("invalid bucket name %q: must start and end with a letter or digit", bucket)
	}
	return nil
}

func isASCIIAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}

func classifyHTTPStatus(operation string, response *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxListResponseBytes))
	cause := fmt.Errorf("%s returned HTTP %d", operation, response.StatusCode)
	switch response.StatusCode {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return &retryableHTTPError{cause: cause, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
	default:
		return cause
	}
}

func isRetryableHTTPError(err error) bool {
	var retryable *retryableHTTPError
	return errors.As(err, &retryable)
}

func parseRetryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		return max(time.Until(retryAt), 0)
	}
	return 0
}

func waitForRetry(ctx context.Context, attempt int, retryErr error) error {
	maximum := 100 * time.Millisecond * time.Duration(1<<attempt)
	delay := time.Duration(rand.Int63n(int64(maximum) + 1))
	var retryable *retryableHTTPError
	if errors.As(retryErr, &retryable) && retryable.retryAfter > delay {
		delay = retryable.retryAfter
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func xmlRoot(reader io.Reader) (xml.Name, error) {
	decoder := xml.NewDecoder(reader)
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return xml.Name{}, errors.New("empty S3 list response")
			}
			return xml.Name{}, err
		}
		if start, ok := token.(xml.StartElement); ok {
			return start.Name, nil
		}
	}
}
