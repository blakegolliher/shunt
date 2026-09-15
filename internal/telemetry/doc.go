// Package telemetry owns the metric catalog (docs/telemetry-catalog.md), the JSON access log,
// and the slow-request ring (docs/DESIGN.md §2.7). A metric not in the catalog fails CI.
// POC scope: the six P1 metrics and the slow ring. Built in POC-1.
package telemetry
