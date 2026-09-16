package s3response

import "time"

// Local stand-ins for the aws-sdk-go-v2 service/s3/types used by the lifted s3response.go.
// Field names match the SDK so encoding/xml emits the same element names; the SDK is a
// test-only dependency of shunt (docs/DESIGN.md §5) and must not reach the proxy binary.

// String enums carry S3 wire values; each is a distinct type so struct fields read as in the SDK.
type (
	// ChecksumType is FULL_OBJECT or COMPOSITE.
	ChecksumType string
	// ChecksumAlgorithm is CRC32, CRC32C, SHA1, SHA256, or CRC64NVME.
	ChecksumAlgorithm string
	// ObjectStorageClass is the storage class as reported in ListObjects.
	ObjectStorageClass string
	// EncodingType is "url" when listing keys are URL-encoded.
	EncodingType string
)

// RestoreStatus is the restore state reported in listings.
type RestoreStatus struct {
	IsRestoreInProgress *bool
	RestoreExpiryDate   *time.Time
}
