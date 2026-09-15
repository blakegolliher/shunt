// Package sigv4 implements SigV4 canonicalization, header and presigned verification,
// aws-chunked decoding, and upstream signing (docs/DESIGN.md §2.2, ADR-0001).
// Verify and sign share one canonicalization. SigV4A is rejected. Built in POC-2.
package sigv4
