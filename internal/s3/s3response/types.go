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
	// StorageClass is an S3 storage class name on a request.
	StorageClass string
	// ObjectStorageClass is the storage class as reported in ListObjects.
	ObjectStorageClass string
	// ObjectVersionStorageClass is the storage class as reported in ListObjectVersions.
	ObjectVersionStorageClass string
	// EncodingType is "url" when listing keys are URL-encoded.
	EncodingType string
	// ServerSideEncryption is AES256 or aws:kms.
	ServerSideEncryption string
	// MFADeleteStatus is Enabled or Disabled.
	MFADeleteStatus string
	// BucketVersioningStatus is Enabled or Suspended.
	BucketVersioningStatus string
	// ObjectLockRetentionMode is GOVERNANCE or COMPLIANCE.
	ObjectLockRetentionMode string
	// ObjectLockMode is GOVERNANCE or COMPLIANCE.
	ObjectLockMode string
	// ObjectLockLegalHoldStatus is ON or OFF.
	ObjectLockLegalHoldStatus string
	// ObjectCannedACL is a canned ACL name such as private.
	ObjectCannedACL string
	// RequestPayer is "requester" when the requester pays.
	RequestPayer string
	// MetadataDirective is COPY or REPLACE.
	MetadataDirective string
	// TaggingDirective is COPY or REPLACE.
	TaggingDirective string
	// ObjectOwnership is BucketOwnerPreferred, ObjectWriter, or BucketOwnerEnforced.
	ObjectOwnership string
)

// Checksum is the checksum block of GetObjectAttributes.
type Checksum struct {
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      ChecksumType
}

// ObjectPart is one part in GetObjectAttributes' ObjectParts.
type ObjectPart struct {
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	PartNumber        *int32
	Size              *int64
}

// RestoreStatus is the restore state reported in listings.
type RestoreStatus struct {
	IsRestoreInProgress *bool
	RestoreExpiryDate   *time.Time
}

// ObjectIdentifier is one object in a DeleteObjects request.
type ObjectIdentifier struct {
	Key              *string
	ETag             *string
	LastModifiedTime *time.Time
	Size             *int64
	VersionId        *string //nolint:revive // S3 wire name
}

// DeletedObject is one entry in a DeleteObjects result.
type DeletedObject struct {
	DeleteMarker          *bool
	DeleteMarkerVersionId *string //nolint:revive // S3 wire name
	Key                   *string
	VersionId             *string //nolint:revive // S3 wire name
}

// Error is one error entry in a DeleteObjects result.
type Error struct {
	Code      *string
	Key       *string
	Message   *string
	VersionId *string //nolint:revive // S3 wire name
}

// CompletedPart is one part in a CompleteMultipartUpload request.
type CompletedPart struct {
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ETag              *string
	PartNumber        *int32
}

// OwnershipControlsRule is one rule in OwnershipControls.
type OwnershipControlsRule struct {
	ObjectOwnership ObjectOwnership
}

// DeleteMarkerEntry is one delete marker in ListObjectVersions.
type DeleteMarkerEntry struct {
	IsLatest     *bool
	Key          *string
	LastModified *time.Time
	Owner        *Owner
	VersionId    *string //nolint:revive // S3 wire name
}
