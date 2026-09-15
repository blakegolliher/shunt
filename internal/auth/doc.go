// Package auth holds the static YAML CredentialStore (docs/DESIGN.md §1.2). The interface itself,
// sigv4.CredentialStore, is one of the two named seams in CLAUDE.md and lives next to the
// verifier that consumes it. Hot reload is deferred (docs/POC.md); STS is v2.
package auth
