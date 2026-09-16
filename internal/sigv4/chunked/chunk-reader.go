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
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-16: removed the unused
// payload-type helpers (IsStreamingPayload, IsUnsignedPaylod, IsUnsignedStreamingPayload,
// IsAnonymousPayloadHashSupported, IsSpecialPayload, IsValidSha256PayloadHeader), NewChunkReader,
// and the checksum-type constants nothing used (G1 simplicity review).

package chunked

import (
	"net/http"
	"strconv"
	"strings"

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
