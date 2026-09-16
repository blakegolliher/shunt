package sigv4

import (
	"encoding/hex"
	"strings"
)

// PayloadMode classifies x-amz-content-sha256 (docs/DESIGN.md §2.2 table).
type PayloadMode uint8

// Payload modes.
const (
	PayloadMissing                  PayloadMode = iota // header absent
	PayloadUnsigned                                    // UNSIGNED-PAYLOAD
	PayloadSHA256                                      // 64 hex chars
	PayloadStreamingSigned                             // STREAMING-AWS4-HMAC-SHA256-PAYLOAD
	PayloadStreamingSignedTrailer                      // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
	PayloadStreamingUnsignedTrailer                    // STREAMING-UNSIGNED-PAYLOAD-TRAILER
	PayloadStreamingECDSA                              // STREAMING-AWS4-ECDSA-* (rejected)
	PayloadInvalid                                     // anything else
)

func (m PayloadMode) String() string {
	switch m {
	case PayloadMissing:
		return "missing"
	case PayloadUnsigned:
		return "unsigned"
	case PayloadSHA256:
		return "sha256"
	case PayloadStreamingSigned:
		return "streaming-signed"
	case PayloadStreamingSignedTrailer:
		return "streaming-signed-trailer"
	case PayloadStreamingUnsignedTrailer:
		return "streaming-unsigned-trailer"
	case PayloadStreamingECDSA:
		return "streaming-ecdsa"
	}
	return "invalid"
}

// Streaming reports whether the body is aws-chunked encoded.
func (m PayloadMode) Streaming() bool {
	return m == PayloadStreamingSigned || m == PayloadStreamingSignedTrailer || m == PayloadStreamingUnsignedTrailer
}

// Decodes reports whether shunt must decode the chunks (signed variants).
func (m PayloadMode) Decodes() bool {
	return m == PayloadStreamingSigned || m == PayloadStreamingSignedTrailer
}

// parsePayloadMode classifies the header value.
func parsePayloadMode(v string) PayloadMode {
	switch v {
	case "":
		return PayloadMissing
	case UnsignedPayload:
		return PayloadUnsigned
	case StreamingSignedPayload:
		return PayloadStreamingSigned
	case StreamingSignedPayloadTrailer:
		return PayloadStreamingSignedTrailer
	case StreamingUnsignedPayloadTrailer:
		return PayloadStreamingUnsignedTrailer
	}
	if strings.HasPrefix(v, "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD") {
		return PayloadStreamingECDSA
	}
	if len(v) == 64 {
		if _, err := hex.DecodeString(v); err == nil {
			return PayloadSHA256
		}
	}
	return PayloadInvalid
}
