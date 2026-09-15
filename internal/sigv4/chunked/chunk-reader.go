// Copyright 2024 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-15: package renamed to chunked;
// fiber.Ctx parameters replaced by http.Header; debuglogger calls removed; s3err replaced by the shunt
// s3 error table; sha512/md5/xxhash checksum types dropped (Versity extensions, not AWS algorithms).

package chunked

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"
)

const (
	maxObjSizeLimit = 5 * 1024 * 1024 * 1024 // 5gb
)

type payloadType string

const (
	payloadTypeUnsigned                 payloadType = "UNSIGNED-PAYLOAD"
	payloadTypeStreamingUnsignedTrailer payloadType = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	payloadTypeStreamingSigned          payloadType = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	payloadTypeStreamingSignedTrailer   payloadType = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	payloadTypeStreamingEcdsa           payloadType = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
	payloadTypeStreamingEcdsaTrailer    payloadType = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
)

func getPayloadTypeNotSupportedErr(p payloadType) error {
	return s3.Error{
		Status:  http.StatusNotImplemented,
		Code:    s3.NotImplemented,
		Message: fmt.Sprintf("The chunk encoding algorithm %v is not supported.", p),
	}
}

var (
	specialValues = map[payloadType]bool{
		payloadTypeUnsigned:                 true,
		payloadTypeStreamingUnsignedTrailer: true,
		payloadTypeStreamingSigned:          true,
		payloadTypeStreamingSignedTrailer:   true,
		payloadTypeStreamingEcdsa:           true,
		payloadTypeStreamingEcdsaTrailer:    true,
	}
)

func (pt payloadType) isValid() bool {
	return pt == payloadTypeUnsigned ||
		pt == payloadTypeStreamingUnsignedTrailer ||
		pt == payloadTypeStreamingSigned ||
		pt == payloadTypeStreamingSignedTrailer ||
		pt == payloadTypeStreamingEcdsa ||
		pt == payloadTypeStreamingEcdsaTrailer
}

type checksumType string

const (
	checksumTypeCrc32     checksumType = "x-amz-checksum-crc32"
	checksumTypeCrc32c    checksumType = "x-amz-checksum-crc32c"
	checksumTypeSha1      checksumType = "x-amz-checksum-sha1"
	checksumTypeSha256    checksumType = "x-amz-checksum-sha256"
	checksumTypeCrc64nvme checksumType = "x-amz-checksum-crc64nvme"
)

func (c checksumType) isValid() bool {
	return c == checksumTypeCrc32 ||
		c == checksumTypeCrc32c ||
		c == checksumTypeSha1 ||
		c == checksumTypeSha256 ||
		c == checksumTypeCrc64nvme
}

// Extracts and validates the checksum type from the 'X-Amz-Trailer' header
func ExtractChecksumType(h http.Header) (checksumType, error) {
	trailer := h.Get("X-Amz-Trailer")
	chType := checksumType(strings.ToLower(trailer))
	if chType != "" && !chType.isValid() {
		return "", errTrailerNotSupported
	}

	return chType, nil
}

// IsSpecialPayload checks for special authorization types
func IsSpecialPayload(str string) bool {
	return specialValues[payloadType(str)]
}

// IsValidSha256PayloadHeader checks if the provided x-amz-content-sha256
// payload header is valid special payload type or a valid sh256 hash
func IsValidSha256PayloadHeader(value string) bool {
	// empty header is valid
	if value == "" {
		return true
	}
	// special values are valid
	if IsSpecialPayload(value) {
		return true
	}

	// check to be a valid sha256
	if len(value) != 64 {
		return false
	}

	// decode the string as hex
	_, err := hex.DecodeString(value)
	return err == nil
}

// Checks if the provided string is unsigned payload trailer type
func IsUnsignedStreamingPayload(str string) bool {
	return payloadType(str) == payloadTypeStreamingUnsignedTrailer
}

// IsAnonymousPayloadHashSupported returns error if payload hash
// is streaming signed.
// e.g.
// "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD" ...
func IsAnonymousPayloadHashSupported(hash string) error {
	switch payloadType(hash) {
	case payloadTypeStreamingEcdsa, payloadTypeStreamingEcdsaTrailer, payloadTypeStreamingSigned, payloadTypeStreamingSignedTrailer:
		return errAnonymousSignedStreaming
	}

	return nil
}

// IsUnsignedPaylod checks if the provided payload hash type
// is "UNSIGNED-PAYLOAD"
func IsUnsignedPaylod(hash string) bool {
	return hash == string(payloadTypeUnsigned)
}

// IsStreamingPayload checks for streaming/unsigned authorization types
func IsStreamingPayload(str string) bool {
	pt := payloadType(str)
	return pt == payloadTypeStreamingUnsignedTrailer ||
		pt == payloadTypeStreamingSigned ||
		pt == payloadTypeStreamingSignedTrailer
}

// ParseDecodedContentLength extracts and validates the
// 'x-amz-decoded-content-length' from fiber context
func ParseDecodedContentLength(h http.Header) (int64, error) {
	decContLengthStr := h.Get("X-Amz-Decoded-Content-Length")
	if decContLengthStr == "" {
		return 0, errMissingDecodedLength
	}
	decContLength, err := strconv.ParseInt(decContLengthStr, 10, 64)
	if err != nil {
		return 0, errMissingDecodedLength
	}

	if decContLength > maxObjSizeLimit {
		return 0, s3.Lookup(s3.EntityTooLarge)
	}

	return decContLength, nil
}

func NewChunkReader(h http.Header, r io.Reader, authdata AuthData, canonicalString string, derivedKey []byte, date time.Time) (io.Reader, error) {
	cLength, err := ParseDecodedContentLength(h)
	if err != nil {
		return nil, err
	}

	contentSha256 := payloadType(h.Get("X-Amz-Content-Sha256"))
	if !contentSha256.isValid() {
		//TODO: Add proper APIError
		return nil, fmt.Errorf("invalid x-amz-content-sha256: %v", string(contentSha256))
	}

	checksumType, err := ExtractChecksumType(h)
	if err != nil {
		return nil, err
	}

	switch contentSha256 {
	case payloadTypeStreamingUnsignedTrailer:
		return NewUnsignedChunkReader(r, checksumType, cLength)
	case payloadTypeStreamingSignedTrailer:
		return NewSignedChunkReader(r, authdata, canonicalString, derivedKey, date, checksumType, true, cLength)
	case payloadTypeStreamingSigned:
		return NewSignedChunkReader(r, authdata, canonicalString, derivedKey, date, "", false, cLength)
	// return not supported for:
	// - STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD
	// - STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER
	default:
		return nil, getPayloadTypeNotSupportedErr(contentSha256)
	}
}
