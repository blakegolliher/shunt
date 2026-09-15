// Package upstream owns the connection to backend clusters: one http.Transport per cluster,
// endpoint selection, and (from P3a) health, load balancing, retries, and ejection
// (docs/DESIGN.md §2.8). POC-1 scope: one static cluster, round-robin over its endpoints.
package upstream
