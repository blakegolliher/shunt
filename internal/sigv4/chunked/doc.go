// Package chunked decodes aws-chunked request bodies: STREAMING-AWS4-HMAC-SHA256-PAYLOAD (signed
// chunks, optional signed trailer) and STREAMING-UNSIGNED-PAYLOAD-TRAILER, verifying chunk
// signatures and trailing checksums while streaming in constant memory, and encodes the unsigned
// trailer form for forwarding (docs/DESIGN.md §2.2). The three reader files are lifted from
// versitygw s3api/utils at 4dc0debf (Apache-2.0; see the Modified-by line in each file,
// THIRD_PARTY_NOTICES, and docs/deps.md). helpers.go and encoder.go are shunt's own.
package chunked
