package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	dnsHeaderLen       = 12
	dnsTypeCNAME       = 5
	dnsClassIN         = 1
	s3Suffix           = "s3.amazonaws.com."
	nonexistentTarget  = "s3-1-w.amazonaws.com."
	maxDNSMessageBytes = 4096
)

var (
	errMalformedResponse = errors.New("malformed DNS response")
	errTruncatedResponse = errors.New("truncated DNS response")
)

func buildQuery(candidate string, id uint16) ([]byte, string, error) {
	qname := candidate + "." + s3Suffix
	encoded, err := encodeName(qname)
	if err != nil {
		return nil, "", err
	}

	message := make([]byte, dnsHeaderLen+len(encoded)+4)
	binary.BigEndian.PutUint16(message[0:2], id)
	binary.BigEndian.PutUint16(message[2:4], 0x0100)
	binary.BigEndian.PutUint16(message[4:6], 1)
	copy(message[dnsHeaderLen:], encoded)
	offset := dnsHeaderLen + len(encoded)
	binary.BigEndian.PutUint16(message[offset:offset+2], dnsTypeCNAME)
	binary.BigEndian.PutUint16(message[offset+2:offset+4], dnsClassIN)
	return message, qname, nil
}

func encodeName(name string) ([]byte, error) {
	if name == "." {
		return []byte{0}, nil
	}
	if !strings.HasSuffix(name, ".") {
		return nil, fmt.Errorf("DNS name must be absolute: %q", name)
	}

	labels := strings.Split(name[:len(name)-1], ".")
	encoded := make([]byte, 0, len(name)+1)
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return nil, fmt.Errorf("invalid DNS label in %q", name)
		}
		encoded = append(encoded, byte(len(label)))
		encoded = append(encoded, label...)
	}
	encoded = append(encoded, 0)
	if len(encoded) > 255 {
		return nil, fmt.Errorf("DNS name is too long: %q", name)
	}
	return encoded, nil
}

func parseResponse(message []byte) (id uint16, question string, found bool, err error) {
	if len(message) < dnsHeaderLen {
		return 0, "", false, errMalformedResponse
	}
	id = binary.BigEndian.Uint16(message[0:2])
	flags := binary.BigEndian.Uint16(message[2:4])
	if flags&0x8000 == 0 {
		return id, "", false, errMalformedResponse
	}
	if flags&0x0200 != 0 {
		return id, "", false, errTruncatedResponse
	}

	questionCount := int(binary.BigEndian.Uint16(message[4:6]))
	answerCount := int(binary.BigEndian.Uint16(message[6:8]))
	if questionCount == 0 {
		return id, "", false, errMalformedResponse
	}

	offset := dnsHeaderLen
	for i := 0; i < questionCount; i++ {
		name, next, decodeErr := decodeName(message, offset)
		if decodeErr != nil || next+4 > len(message) {
			return id, "", false, errMalformedResponse
		}
		if i == 0 {
			question = name
			if binary.BigEndian.Uint16(message[next:next+2]) != dnsTypeCNAME ||
				binary.BigEndian.Uint16(message[next+2:next+4]) != dnsClassIN {
				return id, question, false, errMalformedResponse
			}
		}
		offset = next + 4
	}

	rcode := flags & 0x000f
	if rcode != 0 && rcode != 3 {
		return id, question, false, fmt.Errorf("DNS response code %d", rcode)
	}

	for i := 0; i < answerCount; i++ {
		_, next, decodeErr := decodeName(message, offset)
		if decodeErr != nil || next+10 > len(message) {
			return id, question, false, errMalformedResponse
		}
		recordType := binary.BigEndian.Uint16(message[next : next+2])
		rdataLength := int(binary.BigEndian.Uint16(message[next+8 : next+10]))
		rdataStart := next + 10
		rdataEnd := rdataStart + rdataLength
		if rdataEnd > len(message) {
			return id, question, false, errMalformedResponse
		}

		if recordType == dnsTypeCNAME {
			target, consumed, decodeErr := decodeName(message, rdataStart)
			if decodeErr != nil || consumed > rdataEnd {
				return id, question, false, errMalformedResponse
			}
			if !strings.EqualFold(target, nonexistentTarget) {
				found = true
			}
		}
		offset = rdataEnd
	}
	return id, question, found, nil
}

func decodeName(message []byte, offset int) (string, int, error) {
	if offset < 0 || offset >= len(message) {
		return "", 0, errMalformedResponse
	}

	var name strings.Builder
	next := -1
	jumps := 0
	for {
		if offset >= len(message) {
			return "", 0, errMalformedResponse
		}
		length := int(message[offset])
		switch length & 0xc0 {
		case 0xc0:
			if offset+1 >= len(message) || jumps >= 128 {
				return "", 0, errMalformedResponse
			}
			if next < 0 {
				next = offset + 2
			}
			offset = int(binary.BigEndian.Uint16(message[offset:offset+2]) & 0x3fff)
			jumps++
			continue
		case 0x00:
		default:
			return "", 0, errMalformedResponse
		}

		offset++
		if length == 0 {
			if next < 0 {
				next = offset
			}
			if name.Len() == 0 {
				return ".", next, nil
			}
			return name.String(), next, nil
		}
		if length > 63 || offset+length > len(message) {
			return "", 0, errMalformedResponse
		}
		name.Write(message[offset : offset+length])
		name.WriteByte('.')
		offset += length
	}
}
