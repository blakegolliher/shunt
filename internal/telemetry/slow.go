package telemetry

import (
	"sync"
	"time"
)

// SlowEntry is one slow request with its timing breakdown (docs/DESIGN.md §2.7).
type SlowEntry struct {
	At                time.Time     `json:"at"`
	RequestID         string        `json:"request_id"`
	UpstreamRequestID string        `json:"upstream_request_id"`
	Op                string        `json:"op"`
	Method            string        `json:"method"`
	Bucket            string        `json:"bucket"`
	Status            int           `json:"status"`
	BytesIn           int64         `json:"bytes_in"`
	BytesOut          int64         `json:"bytes_out"`
	Upstream          string        `json:"upstream"`
	Parse             time.Duration `json:"t_parse_ns"`            // request received → classified
	UpstreamConnect   time.Duration `json:"t_upstream_connect_ns"` // dial time, 0 when the connection was reused
	UpstreamTTFB      time.Duration `json:"t_upstream_ttfb_ns"`    // request written → first response byte
	UpstreamBody      time.Duration `json:"t_upstream_body_ns"`    // first response byte → last body byte
	Total             time.Duration `json:"t_total_ns"`
	Error             string        `json:"error,omitempty"`
}

// SlowRing keeps the last N requests over a threshold. Fixed size, no allocation per insert.
type SlowRing struct {
	mu        sync.Mutex
	threshold time.Duration
	entries   []SlowEntry
	next      int
	count     int
}

// NewSlowRing creates a ring of size entries recording requests slower than threshold.
func NewSlowRing(size int, threshold time.Duration) *SlowRing {
	if size < 1 {
		size = 1
	}
	return &SlowRing{threshold: threshold, entries: make([]SlowEntry, size)}
}

// Threshold returns the configured threshold.
func (r *SlowRing) Threshold() time.Duration { return r.threshold }

// Record stores e if its total exceeds the threshold. Returns true when stored.
func (r *SlowRing) Record(e *SlowEntry) bool {
	if e.Total < r.threshold {
		return false
	}
	r.mu.Lock()
	r.entries[r.next] = *e
	r.next = (r.next + 1) % len(r.entries)
	if r.count < len(r.entries) {
		r.count++
	}
	r.mu.Unlock()
	return true
}

// Snapshot returns the stored entries newest first.
func (r *SlowRing) Snapshot() []SlowEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SlowEntry, 0, r.count)
	for i := 1; i <= r.count; i++ {
		idx := (r.next - i + len(r.entries)) % len(r.entries)
		out = append(out, r.entries[idx])
	}
	return out
}
