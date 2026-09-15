// Package proxy is the pipeline: one handler, no middleware framework. It parses and classifies
// the request, forwards it to the cluster with Host preserved (passthrough) and streams both
// bodies with io.CopyBuffer through a pooled fixed-size buffer, the only body-copy primitive
// in the codebase (docs/DESIGN.md §2.1). Late upstream failures abort the client connection so
// a truncated body is never mistaken for a complete one (ADR-0003).
package proxy
