package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestS3ListCheckerRecognizesPublicListing(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path != "/public-bucket" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.URL.Query().Get("list-type") != "2" || request.URL.Query().Get("max-keys") != "1" {
			t.Errorf("unexpected query: %s", request.URL.RawQuery)
		}
		writer.Header().Set("Content-Type", "application/xml")
		_, _ = writer.Write([]byte(`<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>`))
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	listable, err := checker.IsListable(context.Background(), "public-bucket")
	if err != nil {
		t.Fatal(err)
	}
	if !listable {
		t.Fatal("public listing was not recognized")
	}
	if requests.Load() != 1 {
		t.Fatalf("got %d requests, want 1", requests.Load())
	}
}

func TestS3ListCheckerRejectsAccessDenied(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("x-amz-bucket-region", "us-east-1")
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	listable, err := checker.IsListable(context.Background(), "private-bucket")
	if err != nil {
		t.Fatal(err)
	}
	if listable {
		t.Fatal("access-denied bucket was reported as listable")
	}
}

func TestS3ListCheckerRetriesServerError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = writer.Write([]byte(`<ListBucketResult></ListBucketResult>`))
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	listable, err := checker.IsListable(context.Background(), "eventual-bucket")
	if err != nil {
		t.Fatal(err)
	}
	if !listable || requests.Load() != 2 {
		t.Fatalf("listable=%v requests=%d", listable, requests.Load())
	}
}

func TestS3HeadBucketExistenceStatuses(t *testing.T) {
	statuses := map[string]int{
		"public":     http.StatusOK,
		"redirected": http.StatusMovedPermanently,
		"private":    http.StatusForbidden,
		"missing":    http.StatusNotFound,
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", request.Method)
		}
		status := statuses[request.URL.Path[1:]]
		if status == http.StatusForbidden {
			writer.Header().Set("x-amz-bucket-region", "us-east-1")
		}
		writer.WriteHeader(status)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 4)
	checker.endpoint = server.URL
	for bucket, status := range statuses {
		exists, err := checker.ProbeExists(context.Background(), bucket)
		if err != nil {
			t.Fatalf("ProbeExists(%q): %v", bucket, err)
		}
		want := status == http.StatusOK || status == http.StatusMovedPermanently || status == http.StatusForbidden
		if exists != want {
			t.Errorf("ProbeExists(%q) = %v, want %v", bucket, exists, want)
		}
	}
}

func TestS3HeadBucketRejectsInconclusiveForbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	exists, err := checker.ProbeExists(context.Background(), "private-bucket")
	if err == nil || exists {
		t.Fatalf("exists=%v err=%v, want an inconclusive error", exists, err)
	}
	if checker.Requests() != 1 {
		t.Fatalf("non-retryable response made %d requests", checker.Requests())
	}
}

func TestS3HeadBucketUsesRegionAsPositiveEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("x-amz-bucket-region", "eu-west-1")
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	exists, err := checker.ProbeExists(context.Background(), "obscured-bucket")
	if err != nil || !exists {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
}

func TestS3HeadBucketRetriesThrottling(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) < 3 {
			writer.Header().Set("Retry-After", "0")
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	exists, err := checker.ProbeExists(context.Background(), "eventual-bucket")
	if err != nil || exists || requests.Load() != 3 {
		t.Fatalf("exists=%v err=%v requests=%d", exists, err, requests.Load())
	}
}

func TestBucketCandidateValidation(t *testing.T) {
	for _, candidate := range []string{"ab", "-bucket", "bucket-", "bucket/name", "bucket name"} {
		if err := validateBucketCandidate(candidate); err == nil {
			t.Errorf("validateBucketCandidate(%q) succeeded", candidate)
		}
	}
	for _, candidate := range []string{"valid-bucket", "legacy_BUCKET", "bucket.example"} {
		if err := validateBucketCandidate(candidate); err != nil {
			t.Errorf("validateBucketCandidate(%q): %v", candidate, err)
		}
	}
}

func TestS3ListingClassifiesPrivateBucketAsExisting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("x-amz-bucket-region", "us-east-1")
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	checker := newS3ListChecker(time.Second, 1)
	checker.endpoint = server.URL
	exists, listable, err := checker.ProbeListing(context.Background(), "private-bucket")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || listable {
		t.Fatalf("exists=%v listable=%v", exists, listable)
	}
}
