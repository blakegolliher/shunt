package chunked

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/blakegolliher/shunt/internal/s3"
)

// AuthData is what the signed-chunk reader needs from the verified seed request: the access key
// (for error messages), the credential-scope region, and the seed signature.
type AuthData struct {
	Access    string
	Region    string
	Signature string
}

// ChecksumType exposes the trailer checksum type to callers outside the package.
type ChecksumType = checksumType

// Checksum trailer names accepted by the readers and produced by the encoder.
const (
	ChecksumCRC32     = checksumTypeCrc32
	ChecksumCRC32C    = checksumTypeCrc32c
	ChecksumSHA1      = checksumTypeSha1
	ChecksumSHA256    = checksumTypeSha256
	ChecksumCRC64NVME = checksumTypeCrc64nvme
)

// checksumLengths is the decoded byte length of each algorithm's base64 checksum value.
var checksumLengths = map[string]int{
	"CRC32": 4, "CRC32C": 4, "SHA1": 20, "SHA256": 32, "CRC64NVME": 8,
}

// IsValidChecksum reports whether checksum is base64 of the right length for algorithm
// (algorithm is the upper-case name without the x-amz-checksum- prefix).
func IsValidChecksum(checksum, algorithm string) bool {
	want, ok := checksumLengths[strings.ToUpper(algorithm)]
	if !ok {
		return false
	}
	b, err := base64.StdEncoding.DecodeString(checksum)
	return err == nil && len(b) == want
}

// Algorithm returns the upper-case algorithm name of a checksum trailer type.
func Algorithm(ct checksumType) string {
	return strings.ToUpper(strings.TrimPrefix(string(ct), "x-amz-checksum-"))
}

func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Errors the readers return, expressed in shunt's S3 error table.
var (
	errContentLengthMismatch = s3.Error{Code: s3.IncompleteBody, Status: http.StatusBadRequest,
		Message: "The request body did not match x-amz-decoded-content-length."}
	errMissingDecodedLength = s3.Error{Code: s3.MissingContentLength, Status: http.StatusLengthRequired,
		Message: "You must provide a valid x-amz-decoded-content-length header for aws-chunked content."}
	errTrailerNotSupported = s3.Error{Code: s3.InvalidRequest, Status: http.StatusBadRequest,
		Message: "The value specified in the x-amz-trailer header is not supported."}
	errAnonymousSignedStreaming = s3.Error{Code: s3.InvalidRequest, Status: http.StatusBadRequest,
		Message: "Anonymous requests may not use signed streaming payloads. Please use UNSIGNED-PAYLOAD or STREAMING-UNSIGNED-PAYLOAD-TRAILER."}
)

func errBadDigest(algorithm string) error {
	return s3.Error{Code: s3.BadDigest, Status: http.StatusBadRequest,
		Message: fmt.Sprintf("The %s you specified did not match what we received.", strings.ToUpper(algorithm))}
}

func errInvalidTrailingChecksum(trailer string) error {
	return s3.Error{Code: s3.InvalidRequest, Status: http.StatusBadRequest,
		Message: fmt.Sprintf("Value for %s trailing header is invalid.", trailer)}
}

func errInvalidChunkSize(chunk int, size int64) error {
	return s3.Error{Code: s3.InvalidChunkSizeError, Status: http.StatusBadRequest,
		Message: fmt.Sprintf("Only the last chunk is allowed to have a size less than 8192 bytes (chunk %d had %d).", chunk, size)}
}

// DataRead reports how many decoded bytes the signed reader has handed out.
func (cr *ChunkReader) DataRead() int64 { return cr.dataRead }
