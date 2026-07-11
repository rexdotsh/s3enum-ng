package main

import (
	"encoding/binary"
	"testing"
)

func dnsResponse(t *testing.T, candidate, target string) []byte {
	t.Helper()
	query, _, err := buildQuery(candidate, 0x1234)
	if err != nil {
		t.Fatal(err)
	}
	response := append([]byte(nil), query...)
	binary.BigEndian.PutUint16(response[2:4], 0x8180)
	binary.BigEndian.PutUint16(response[6:8], 1)

	encodedTarget, err := encodeName(target)
	if err != nil {
		t.Fatal(err)
	}
	answer := make([]byte, 12+len(encodedTarget))
	binary.BigEndian.PutUint16(answer[0:2], 0xc00c)
	binary.BigEndian.PutUint16(answer[2:4], dnsTypeCNAME)
	binary.BigEndian.PutUint16(answer[4:6], dnsClassIN)
	binary.BigEndian.PutUint32(answer[6:10], 60)
	binary.BigEndian.PutUint16(answer[10:12], uint16(len(encodedTarget)))
	copy(answer[12:], encodedTarget)
	return append(response, answer...)
}

func TestParseResponseDetectsExistingBucket(t *testing.T) {
	message := dnsResponse(t, "exists", "s3-us-west-2-w.amazonaws.com.")
	id, question, found, err := parseResponse(message)
	if err != nil {
		t.Fatal(err)
	}
	if id != 0x1234 || question != "exists.s3.amazonaws.com." || !found {
		t.Fatalf("id=%x question=%q found=%v", id, question, found)
	}
}

func TestParseResponseRejectsNonexistentBucketTarget(t *testing.T) {
	message := dnsResponse(t, "missing", nonexistentTarget)
	_, _, found, err := parseResponse(message)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("nonexistent bucket was reported as existing")
	}
}

func TestBuildQueryRejectsInvalidNames(t *testing.T) {
	if _, _, err := buildQuery("bad..name", 1); err == nil {
		t.Fatal("expected invalid label error")
	}
	if _, _, err := buildQuery(string(make([]byte, 64)), 1); err == nil {
		t.Fatal("expected oversized label error")
	}
}
