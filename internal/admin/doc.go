// Package admin serves /-/healthz, /-/metrics, /-/slow, /-/drain, and /debug/pprof on a separate
// listener (docs/DESIGN.md §1.2). /-/ready, /-/routes, and /-/upstreams arrive with P3a/P4.
package admin
