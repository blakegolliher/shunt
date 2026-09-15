// Package s3response holds the S3 response and request XML structs (ListObjectsV2, ListParts,
// InitiateMultipartUpload, ListBuckets, …). s3response.go is lifted from versitygw
// (github.com/versity/versitygw/s3response at 4dc0debf, Apache-2.0; see THIRD_PARTY_NOTICES and
// docs/deps.md); types.go is shunt's own replacement for the aws-sdk types it referenced.
// These structs feed the uploadId rewrites, ListBuckets synthesis, and the listing merge.
package s3response
