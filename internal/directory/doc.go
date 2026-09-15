// Package directory maps (tenant, bucket) to a placement: which clusters hold the bucket, under
// which backend bucket names, and in which migration state (docs/DESIGN.md §2.3, §2.5).
// Directory is one of the two named interface seams; FileDir is the file backend used by the POC
// and single-site deployments (ADR-0005). Postgres behind shunt-control replaces it in P3c.
package directory
