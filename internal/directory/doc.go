// Package directory defines Directory, the second named interface seam: (tenant, bucket) →
// placement with clusters, backend bucket names, state, and ramp rules (docs/DESIGN.md §2.3).
// POC scope: the file backend only; the control-snapshot backend is P3c. Built in POC-3.
package directory
