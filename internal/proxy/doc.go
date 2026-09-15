// Package proxy is the pipeline: one handler, no middleware framework. It streams bodies in both
// directions with io.CopyBuffer through a pooled fixed-size buffer, the only body-copy primitive in
// the codebase (docs/DESIGN.md §2.1). Built in POC-1.
package proxy
