// Package telemetry owns the metric catalog (docs/telemetry-catalog.md), the JSON access log,
// the slow-request ring, and request ids (docs/DESIGN.md §2.7). A metric not in the catalog
// fails CI (catalog_test.go). POC scope: the six P1 metrics and the slow ring; traces,
// metering, and the full catalog are P4.
package telemetry
