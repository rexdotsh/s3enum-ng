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

type bucketListChecker interface {
	IsListable(context.Context, string) (bool, error)
}

type s3ListChecker struct {
	client   *http.Client
	endpoint string
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
	listable, region, err := checker.checkWithRetry(ctx, checker.endpoint, bucket)
	if err != nil || listable || region == "" {
		return listable, err
	}
	return checker.checkRegional(ctx, region, bucket)
}

func (checker *s3ListChecker) checkRegional(ctx context.Context, region, bucket string) (bool, error) {
	endpoint := "https://s3." + region + ".amazonaws.com"
	listable, _, err := checker.checkWithRetry(ctx, endpoint, bucket)
	return listable, err
}

func (checker *s3ListChecker) checkWithRetry(ctx context.Context, endpoint, bucket string) (bool, string, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		listable, region, err := checker.check(ctx, endpoint, bucket)
		if err == nil {
			return listable, region, nil
		}
		lastErr = err
		if attempt == 0 {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return false, "", ctx.Err()
			}
		}
	}
	return false, "", lastErr
}

func (checker *s3ListChecker) check(ctx context.Context, endpoint, bucket string) (bool, string, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return false, "", err
	}
	target.Path = strings.TrimSuffix(target.Path, "/") + "/" + bucket
	query := target.Query()
	query.Set("list-type", "2")
	query.Set("max-keys", "1")
	target.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false, "", err
	}
	request.Header.Set("User-Agent", "s3enum-ng/"+version)
	response, err := checker.client.Do(request)
	if err != nil {
		return false, "", err
	}
	defer response.Body.Close()

	region := response.Header.Get("x-amz-bucket-region")
	switch response.StatusCode {
	case http.StatusOK:
		root, err := xmlRoot(io.LimitReader(response.Body, maxListResponseBytes))
		if err != nil {
			return false, region, err
		}
		return root.Local == "ListBucketResult", region, nil
	case http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return false, region, nil
	case http.StatusForbidden, http.StatusNotFound:
		return false, region, nil
	default:
		return false, region, fmt.Errorf("S3 list request returned HTTP %d", response.StatusCode)
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
