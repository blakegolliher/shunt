// Package sigv4 implements AWS Signature Version 4 for S3 (docs/DESIGN.md §2.2, ADR-0001):
// canonicalization with the S3 rules, header and presigned verification, and upstream signing.
// Verify and sign share one canonicalization; the AWS SDK signer is used only in tests as the
// oracle. SigV4A is rejected. Sub-package chunked handles aws-chunked bodies.
package sigv4
