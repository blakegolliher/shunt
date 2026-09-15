package telemetry

import (
	"crypto/rand"
	"encoding/hex"
)

// HeaderRequestID is shunt's own request id header, distinct from the upstream's x-amz-request-id.
const HeaderRequestID = "X-Shunt-Request-Id"

// NewRequestID returns 16 random bytes as 32 hex characters.
func NewRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand never fails on Linux
	return hex.EncodeToString(b[:])
}
