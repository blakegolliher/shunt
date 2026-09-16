// Package control is the control API (docs/reference/control-api.md, ADR-0008): one HTTP handler
// per operator action, served under /v1/ on the admin listener. It changes the directory through
// its write path and reaches backends through the proxy's own live clusters. P3c keeps this API
// and replaces the file directory behind it with Postgres (docs/DESIGN.md §1.5).
package control
