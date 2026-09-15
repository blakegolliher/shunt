package telemetry

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestRequestID(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if len(a) != 32 || a == b {
		t.Fatalf("bad ids %q %q", a, b)
	}
}

func TestStatusClass(t *testing.T) {
	for in, want := range map[int]string{200: "2xx", 204: "2xx", 301: "3xx", 404: "4xx", 503: "5xx", 0: "0", 999: "0"} {
		if got := StatusClass(in); got != want {
			t.Errorf("StatusClass(%d)=%q want %q", in, got, want)
		}
	}
}

func TestDurationBuckets(t *testing.T) {
	if len(durationBuckets) != 16 || durationBuckets[0] != 0.001 || durationBuckets[15] < 59.999 || durationBuckets[15] > 60.001 {
		t.Fatalf("buckets: %v", durationBuckets)
	}
	for i := 1; i < len(durationBuckets); i++ {
		if durationBuckets[i] <= durationBuckets[i-1] {
			t.Fatal("buckets not increasing")
		}
	}
}

func TestAccessLogSchema(t *testing.T) {
	var buf bytes.Buffer
	l := NewAccessLogger(&buf)
	l.Log(&Access{RequestID: "r1", Method: "GET", Op: "GetObject", Status: 200, BytesOut: 5, Duration: 1500 * time.Microsecond, Bucket: "b"})
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("%v: %s", err, buf.String())
	}
	for _, k := range []string{"ts", "request_id", "upstream_request_id", "client", "method", "host", "style", "bucket", "op", "status", "bytes_in", "bytes_out", "duration_ms", "ttfb_ms", "upstream", "cluster", "tls", "error"} {
		if _, ok := rec[k]; !ok {
			t.Errorf("missing field %s in %s", k, buf.String())
		}
	}
	for _, absent := range []string{"level", "msg", "key"} {
		if _, ok := rec[absent]; ok {
			t.Errorf("unexpected field %s", absent)
		}
	}
	if rec["duration_ms"].(float64) != 1.5 {
		t.Errorf("duration_ms: %v", rec["duration_ms"])
	}
	NewAccessLogger(nil).Log(&Access{}) // nil writer is a no-op
}

func TestSlowRing(t *testing.T) {
	r := NewSlowRing(3, 10*time.Millisecond)
	if r.Record(&SlowEntry{RequestID: "fast", Total: 1 * time.Millisecond}) {
		t.Fatal("fast request recorded")
	}
	for i := 1; i <= 5; i++ {
		r.Record(&SlowEntry{RequestID: string(rune('0' + i)), Total: time.Duration(i) * 20 * time.Millisecond})
	}
	got := r.Snapshot()
	if len(got) != 3 || got[0].RequestID != "5" || got[1].RequestID != "4" || got[2].RequestID != "3" {
		t.Fatalf("snapshot: %+v", got)
	}
	if NewSlowRing(0, 0) == nil || len(NewSlowRing(0, 0).entries) != 1 {
		t.Fatal("size floor")
	}
}

func BenchmarkSlowRingRecord(b *testing.B) {
	r := NewSlowRing(100, 0)
	e := &SlowEntry{RequestID: "x", Total: time.Second}
	b.ReportAllocs()
	for b.Loop() {
		r.Record(e)
	}
}

func BenchmarkAccessLog(b *testing.B) {
	l := NewAccessLogger(&bytes.Buffer{})
	rec := &Access{RequestID: "0123456789abcdef0123456789abcdef", Method: "GET", Op: "GetObject", Status: 200, BytesOut: 1 << 20, Duration: time.Millisecond}
	b.ReportAllocs()
	for b.Loop() {
		l.Log(rec)
	}
}

func BenchmarkRequestID(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = NewRequestID()
	}
}
