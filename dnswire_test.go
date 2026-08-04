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

func dnsResponseWithOwner(t *testing.T, candidate, owner, target string) []byte {
	t.Helper()
	query, _, err := buildQuery(candidate, 0x1234)
	if err != nil {
		t.Fatal(err)
	}
	response := append([]byte(nil), query...)
	binary.BigEndian.PutUint16(response[2:4], 0x8180)
	binary.BigEndian.PutUint16(response[6:8], 1)

	encodedOwner, err := encodeName(owner)
	if err != nil {
		t.Fatal(err)
	}
	encodedTarget, err := encodeName(target)
	if err != nil {
		t.Fatal(err)
	}
	answer := make([]byte, len(encodedOwner)+10+len(encodedTarget))
	copy(answer, encodedOwner)
	offset := len(encodedOwner)
	binary.BigEndian.PutUint16(answer[offset:offset+2], dnsTypeCNAME)
	binary.BigEndian.PutUint16(answer[offset+2:offset+4], dnsClassIN)
	binary.BigEndian.PutUint16(answer[offset+8:offset+10], uint16(len(encodedTarget)))
	copy(answer[offset+10:], encodedTarget)
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

func TestParseResponseIgnoresUnrelatedAnswers(t *testing.T) {
	message := dnsResponseWithOwner(t, "missing", "unrelated.s3.amazonaws.com.", "regional.amazonaws.com.")
	_, _, found, err := parseResponse(message)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("an unrelated CNAME was reported as the candidate")
	}
}

func TestParseResponseRejectsWrongAnswerClass(t *testing.T) {
	message := dnsResponse(t, "exists", "regional.amazonaws.com.")
	answerOffset := len(message) - (12 + len("\x08regional\x09amazonaws\x03com\x00"))
	binary.BigEndian.PutUint16(message[answerOffset+4:answerOffset+6], 3)
	if _, _, _, err := parseResponse(message); err == nil {
		t.Fatal("expected an invalid answer class error")
	}
}

func TestParseResponseRejectsCNAMEDataPastDeclaredName(t *testing.T) {
	message := dnsResponse(t, "exists", "regional.amazonaws.com.")
	answerOffset := len(message) - (12 + len("\x08regional\x09amazonaws\x03com\x00"))
	rdataLength := binary.BigEndian.Uint16(message[answerOffset+10 : answerOffset+12])
	binary.BigEndian.PutUint16(message[answerOffset+10:answerOffset+12], rdataLength+1)
	message = append(message, 0)
	if _, _, _, err := parseResponse(message); err == nil {
		t.Fatal("expected malformed CNAME data error")
	}
}

func TestParseResponseRejectsNonQueryOpcode(t *testing.T) {
	message := dnsResponse(t, "exists", "regional.amazonaws.com.")
	flags := binary.BigEndian.Uint16(message[2:4])
	binary.BigEndian.PutUint16(message[2:4], flags|0x0800)
	if _, _, _, err := parseResponse(message); err == nil {
		t.Fatal("expected invalid opcode error")
	}
}

func FuzzParseResponse(f *testing.F) {
	query, _, err := buildQuery("seed", 0x1234)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(query)
	f.Add([]byte{})
	f.Fuzz(func(_ *testing.T, message []byte) {
		_, _, _, _ = parseResponse(message)
	})
}
