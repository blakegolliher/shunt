package s3

// Op identifies an S3 API operation. OpUnknown is still proxied but metered separately.
type Op uint16

// Operations, named after the AWS API reference. The order here is arbitrary; the classifier
// table in classify.go decides precedence.
const (
	OpUnknown   Op = iota
	OpPreflight    // CORS preflight (OPTIONS) at any level

	// Service level.
	OpListBuckets

	// Bucket level.
	OpCreateBucket
	OpDeleteBucket
	OpHeadBucket
	OpListObjects
	OpListObjectsV2
	OpListObjectVersions
	OpListMultipartUploads
	OpDeleteObjects
	OpPostObject
	OpGetBucketLocation
	OpGetBucketAcl //nolint:revive // AWS API name
	OpPutBucketAcl //nolint:revive // AWS API name
	OpGetBucketCors
	OpPutBucketCors
	OpDeleteBucketCors
	OpGetBucketLifecycleConfiguration
	OpPutBucketLifecycleConfiguration
	OpDeleteBucketLifecycle
	OpGetBucketLogging
	OpPutBucketLogging
	OpGetBucketNotificationConfiguration
	OpPutBucketNotificationConfiguration
	OpGetObjectLockConfiguration
	OpPutObjectLockConfiguration
	OpGetBucketOwnershipControls
	OpPutBucketOwnershipControls
	OpDeleteBucketOwnershipControls
	OpGetBucketPolicy
	OpPutBucketPolicy
	OpDeleteBucketPolicy
	OpGetBucketPolicyStatus
	OpGetBucketReplication
	OpPutBucketReplication
	OpDeleteBucketReplication
	OpGetBucketRequestPayment
	OpPutBucketRequestPayment
	OpGetBucketTagging
	OpPutBucketTagging
	OpDeleteBucketTagging
	OpGetBucketVersioning
	OpPutBucketVersioning
	OpGetBucketWebsite
	OpPutBucketWebsite
	OpDeleteBucketWebsite
	OpGetBucketEncryption
	OpPutBucketEncryption
	OpDeleteBucketEncryption
	OpGetBucketAccelerateConfiguration
	OpPutBucketAccelerateConfiguration
	OpGetPublicAccessBlock
	OpPutPublicAccessBlock
	OpDeletePublicAccessBlock
	OpGetBucketAnalyticsConfiguration
	OpPutBucketAnalyticsConfiguration
	OpDeleteBucketAnalyticsConfiguration
	OpListBucketAnalyticsConfigurations
	OpGetBucketIntelligentTieringConfiguration
	OpPutBucketIntelligentTieringConfiguration
	OpDeleteBucketIntelligentTieringConfiguration
	OpListBucketIntelligentTieringConfigurations
	OpGetBucketInventoryConfiguration
	OpPutBucketInventoryConfiguration
	OpDeleteBucketInventoryConfiguration
	OpListBucketInventoryConfigurations
	OpGetBucketMetricsConfiguration
	OpPutBucketMetricsConfiguration
	OpDeleteBucketMetricsConfiguration
	OpListBucketMetricsConfigurations

	// Object level.
	OpGetObject
	OpHeadObject
	OpPutObject
	OpCopyObject
	OpDeleteObject
	OpGetObjectAcl //nolint:revive // AWS API name
	OpPutObjectAcl //nolint:revive // AWS API name
	OpGetObjectAttributes
	OpGetObjectTagging
	OpPutObjectTagging
	OpDeleteObjectTagging
	OpGetObjectLegalHold
	OpPutObjectLegalHold
	OpGetObjectRetention
	OpPutObjectRetention
	OpGetObjectTorrent
	OpRestoreObject
	OpSelectObjectContent
	OpCreateMultipartUpload
	OpUploadPart
	OpUploadPartCopy
	OpCompleteMultipartUpload
	OpAbortMultipartUpload
	OpListParts

	opCount
)

var opNames = [...]string{
	OpUnknown:   "Unknown",
	OpPreflight: "Preflight",

	OpListBuckets: "ListBuckets",

	OpCreateBucket:                                "CreateBucket",
	OpDeleteBucket:                                "DeleteBucket",
	OpHeadBucket:                                  "HeadBucket",
	OpListObjects:                                 "ListObjects",
	OpListObjectsV2:                               "ListObjectsV2",
	OpListObjectVersions:                          "ListObjectVersions",
	OpListMultipartUploads:                        "ListMultipartUploads",
	OpDeleteObjects:                               "DeleteObjects",
	OpPostObject:                                  "PostObject",
	OpGetBucketLocation:                           "GetBucketLocation",
	OpGetBucketAcl:                                "GetBucketAcl",
	OpPutBucketAcl:                                "PutBucketAcl",
	OpGetBucketCors:                               "GetBucketCors",
	OpPutBucketCors:                               "PutBucketCors",
	OpDeleteBucketCors:                            "DeleteBucketCors",
	OpGetBucketLifecycleConfiguration:             "GetBucketLifecycleConfiguration",
	OpPutBucketLifecycleConfiguration:             "PutBucketLifecycleConfiguration",
	OpDeleteBucketLifecycle:                       "DeleteBucketLifecycle",
	OpGetBucketLogging:                            "GetBucketLogging",
	OpPutBucketLogging:                            "PutBucketLogging",
	OpGetBucketNotificationConfiguration:          "GetBucketNotificationConfiguration",
	OpPutBucketNotificationConfiguration:          "PutBucketNotificationConfiguration",
	OpGetObjectLockConfiguration:                  "GetObjectLockConfiguration",
	OpPutObjectLockConfiguration:                  "PutObjectLockConfiguration",
	OpGetBucketOwnershipControls:                  "GetBucketOwnershipControls",
	OpPutBucketOwnershipControls:                  "PutBucketOwnershipControls",
	OpDeleteBucketOwnershipControls:               "DeleteBucketOwnershipControls",
	OpGetBucketPolicy:                             "GetBucketPolicy",
	OpPutBucketPolicy:                             "PutBucketPolicy",
	OpDeleteBucketPolicy:                          "DeleteBucketPolicy",
	OpGetBucketPolicyStatus:                       "GetBucketPolicyStatus",
	OpGetBucketReplication:                        "GetBucketReplication",
	OpPutBucketReplication:                        "PutBucketReplication",
	OpDeleteBucketReplication:                     "DeleteBucketReplication",
	OpGetBucketRequestPayment:                     "GetBucketRequestPayment",
	OpPutBucketRequestPayment:                     "PutBucketRequestPayment",
	OpGetBucketTagging:                            "GetBucketTagging",
	OpPutBucketTagging:                            "PutBucketTagging",
	OpDeleteBucketTagging:                         "DeleteBucketTagging",
	OpGetBucketVersioning:                         "GetBucketVersioning",
	OpPutBucketVersioning:                         "PutBucketVersioning",
	OpGetBucketWebsite:                            "GetBucketWebsite",
	OpPutBucketWebsite:                            "PutBucketWebsite",
	OpDeleteBucketWebsite:                         "DeleteBucketWebsite",
	OpGetBucketEncryption:                         "GetBucketEncryption",
	OpPutBucketEncryption:                         "PutBucketEncryption",
	OpDeleteBucketEncryption:                      "DeleteBucketEncryption",
	OpGetBucketAccelerateConfiguration:            "GetBucketAccelerateConfiguration",
	OpPutBucketAccelerateConfiguration:            "PutBucketAccelerateConfiguration",
	OpGetPublicAccessBlock:                        "GetPublicAccessBlock",
	OpPutPublicAccessBlock:                        "PutPublicAccessBlock",
	OpDeletePublicAccessBlock:                     "DeletePublicAccessBlock",
	OpGetBucketAnalyticsConfiguration:             "GetBucketAnalyticsConfiguration",
	OpPutBucketAnalyticsConfiguration:             "PutBucketAnalyticsConfiguration",
	OpDeleteBucketAnalyticsConfiguration:          "DeleteBucketAnalyticsConfiguration",
	OpListBucketAnalyticsConfigurations:           "ListBucketAnalyticsConfigurations",
	OpGetBucketIntelligentTieringConfiguration:    "GetBucketIntelligentTieringConfiguration",
	OpPutBucketIntelligentTieringConfiguration:    "PutBucketIntelligentTieringConfiguration",
	OpDeleteBucketIntelligentTieringConfiguration: "DeleteBucketIntelligentTieringConfiguration",
	OpListBucketIntelligentTieringConfigurations:  "ListBucketIntelligentTieringConfigurations",
	OpGetBucketInventoryConfiguration:             "GetBucketInventoryConfiguration",
	OpPutBucketInventoryConfiguration:             "PutBucketInventoryConfiguration",
	OpDeleteBucketInventoryConfiguration:          "DeleteBucketInventoryConfiguration",
	OpListBucketInventoryConfigurations:           "ListBucketInventoryConfigurations",
	OpGetBucketMetricsConfiguration:               "GetBucketMetricsConfiguration",
	OpPutBucketMetricsConfiguration:               "PutBucketMetricsConfiguration",
	OpDeleteBucketMetricsConfiguration:            "DeleteBucketMetricsConfiguration",
	OpListBucketMetricsConfigurations:             "ListBucketMetricsConfigurations",

	OpGetObject:               "GetObject",
	OpHeadObject:              "HeadObject",
	OpPutObject:               "PutObject",
	OpCopyObject:              "CopyObject",
	OpDeleteObject:            "DeleteObject",
	OpGetObjectAcl:            "GetObjectAcl",
	OpPutObjectAcl:            "PutObjectAcl",
	OpGetObjectAttributes:     "GetObjectAttributes",
	OpGetObjectTagging:        "GetObjectTagging",
	OpPutObjectTagging:        "PutObjectTagging",
	OpDeleteObjectTagging:     "DeleteObjectTagging",
	OpGetObjectLegalHold:      "GetObjectLegalHold",
	OpPutObjectLegalHold:      "PutObjectLegalHold",
	OpGetObjectRetention:      "GetObjectRetention",
	OpPutObjectRetention:      "PutObjectRetention",
	OpGetObjectTorrent:        "GetObjectTorrent",
	OpRestoreObject:           "RestoreObject",
	OpSelectObjectContent:     "SelectObjectContent",
	OpCreateMultipartUpload:   "CreateMultipartUpload",
	OpUploadPart:              "UploadPart",
	OpUploadPartCopy:          "UploadPartCopy",
	OpCompleteMultipartUpload: "CompleteMultipartUpload",
	OpAbortMultipartUpload:    "AbortMultipartUpload",
	OpListParts:               "ListParts",
}

// Ops lists every operation the classifier can produce, in table order. Used by the routing
// completeness test: an operation added without a routing class fails the build.
func Ops() []Op {
	out := make([]Op, 0, opCount)
	for o := Op(0); o < opCount; o++ {
		if opNames[o] != "" {
			out = append(out, o)
		}
	}
	return out
}

// String returns the AWS API name, used as the `op` metric label.
func (o Op) String() string {
	if o < opCount && opNames[o] != "" {
		return opNames[o]
	}
	return "Unknown"
}

// IsData reports whether the operation streams a body for which a total deadline is wrong
// (docs/DESIGN.md §2.8): these get an idle-progress deadline instead.
func (o Op) IsData() bool {
	switch o {
	case OpGetObject, OpPutObject, OpUploadPart, OpPostObject, OpSelectObjectContent,
		OpCopyObject, OpUploadPartCopy, OpUnknown:
		return true
	}
	return false
}

// IsUnknown reports whether the classifier did not recognize the request.
func (o Op) IsUnknown() bool { return o == OpUnknown }
