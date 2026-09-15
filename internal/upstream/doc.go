// Package upstream owns per-endpoint connection pools, health, load balancing, retries, and
// timeouts (docs/DESIGN.md §2.8). POC scope: static endpoints, round-robin, skip-on-connect-error.
// P2C/EWMA, ejection, and dns mode are P3a/P3b. Built in POC-1 and POC-3.
package upstream
