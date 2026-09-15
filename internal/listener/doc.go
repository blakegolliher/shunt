// Package listener owns TCP accept and TLS termination for the client-facing endpoint:
// certificate loading, SNI map, TLS 1.2 minimum (docs/DESIGN.md §1.2, §2.9).
// POC scope (docs/POC.md): TLS from files only. Hot reload, mTLS, SO_REUSEPORT, and PROXY
// protocol are deferred to P1/P3a. Nothing is built here until POC-1.
package listener
