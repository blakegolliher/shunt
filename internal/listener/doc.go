// Package listener owns TCP accept and TLS termination for the client-facing endpoint:
// certificates from files, an SNI map, TLS 1.2 minimum, Go's cipher ordering untouched,
// HTTP/1.1 only (docs/DESIGN.md §2.9). Hot reload, mTLS, SO_REUSEPORT, and PROXY protocol are
// deferred to P1/P3a (docs/POC.md).
package listener
