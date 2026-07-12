package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxListResponseBytes = 64 * 1024

type s3ListChecker struct {
	client   *http.Client
	endpoint string
}

type listingProbe struct {
	exists   bool
	listable bool
	region   string
	redirect bool
}

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

func (checker *s3ListChecker) ProbeExists(ctx context.Context, bucket string) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		exists, err := checker.headBucket(ctx, bucket)
		if err == nil {
			return exists, nil
		}
		lastErr = err
		if attempt == 0 {
			if err := waitForRetry(ctx); err != nil {
				return false, err
			}
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
	request.Header.Set("User-Agent", "s3enum-ng/"+version)
	response, err := checker.client.Do(request)
	if err != nil {
		return false, err
	}
	response.Body.Close()

	region := response.Header.Get("x-amz-bucket-region")
	switch response.StatusCode {
	case http.StatusOK, http.StatusMovedPermanently, http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect, http.StatusForbidden:
		return true, nil
	case http.StatusBadRequest:
		return region != "", nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("S3 HeadBucket returned HTTP %d", response.StatusCode)
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
	for attempt := 0; attempt < 2; attempt++ {
		result, err := checker.probeListing(ctx, endpoint, bucket)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt == 0 {
			if err := waitForRetry(ctx); err != nil {
				return listingProbe{}, err
			}
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
	request.Header.Set("User-Agent", "s3enum-ng/"+version)
	response, err := checker.client.Do(request)
	if err != nil {
		return listingProbe{}, err
	}
	defer response.Body.Close()

	region := response.Header.Get("x-amz-bucket-region")
	switch response.StatusCode {
	case http.StatusOK:
		root, err := xmlRoot(io.LimitReader(response.Body, maxListResponseBytes))
		if err != nil {
			return listingProbe{}, err
		}
		return listingProbe{exists: true, listable: root.Local == "ListBucketResult", region: region}, nil
	case http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return listingProbe{exists: true, region: region, redirect: true}, nil
	case http.StatusForbidden:
		return listingProbe{exists: true, region: region}, nil
	case http.StatusBadRequest:
		return listingProbe{exists: region != "", region: region}, nil
	case http.StatusNotFound:
		return listingProbe{}, nil
	default:
		return listingProbe{}, fmt.Errorf("S3 list request returned HTTP %d", response.StatusCode)
	}
}

func bucketURL(endpoint, bucket string, listing bool) (string, error) {
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

func waitForRetry(ctx context.Context) error {
	select {
	case <-time.After(100 * time.Millisecond):
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
