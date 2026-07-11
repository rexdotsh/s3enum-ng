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
