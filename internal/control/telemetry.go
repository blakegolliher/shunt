package control

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

// TelemetryEvent announces that one completed proxy window has been merged and is ready through
// the telemetry read models.
type TelemetryEvent struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Scopes int       `json:"scopes"`
}

// LatestTelemetry is GET /v1/telemetry/latest.
type LatestTelemetry struct {
	Windows []telemetry.ScopeWindow `json:"windows"`
}

// TelemetrySeries is GET /v1/telemetry/series.
type TelemetrySeries struct {
	Scope  string            `json:"scope"`
	Series string            `json:"series"`
	Op     telemetry.OpClass `json:"op"`
	Points []TelemetryPoint  `json:"points"`
}

// TelemetryPoint is either one emitted latency summary or one exact-counter rate. Percentile
// fields and Value are mutually exclusive on the wire.
type TelemetryPoint struct {
	Start  time.Time         `json:"start"`
	End    time.Time         `json:"end"`
	Series string            `json:"series"`
	Op     telemetry.OpClass `json:"op"`
	Count  int64             `json:"count,omitempty"`
	P50    int64             `json:"p50_us,omitempty"`
	P90    int64             `json:"p90_us,omitempty"`
	P99    int64             `json:"p99_us,omitempty"`
	P999   int64             `json:"p999_us,omitempty"`
	Max    int64             `json:"max_us,omitempty"`
	Value  *float64          `json:"value,omitempty"`
}

func (s *Server) publishTelemetry(ms []Member) error {
	if s.Telemetry == nil {
		return nil
	}
	in := make([]telemetry.MemberWindow, 0, len(ms))
	for _, m := range ms {
		in = append(in, telemetry.MemberWindow{ID: m.ID, Live: m.Live, Telemetry: m.Telemetry})
	}
	started := time.Now()
	merged, err := s.Telemetry.Ingest(in)
	if err != nil {
		return fmt.Errorf("telemetry merge: %w", err)
	}
	if len(merged) > 0 && s.Metrics != nil {
		s.Metrics.TelemetryMerge.Observe(time.Since(started).Seconds() / float64(len(merged)))
	}
	for _, start := range merged {
		if s.Events != nil {
			s.Events.Publish(eventTypeTelemetry, TelemetryEvent{Start: start, End: start.Add(telemetry.WindowDuration), Scopes: len(s.Telemetry.Latest())})
		}
	}
	return nil
}

func (s *Server) flushLocalTelemetry() error {
	if s.Telemetry == nil {
		return nil
	}
	started := time.Now()
	merged, err := s.Telemetry.FlushLocal()
	if err != nil {
		return err
	}
	if len(merged) > 0 && s.Metrics != nil {
		s.Metrics.TelemetryMerge.Observe(time.Since(started).Seconds() / float64(len(merged)))
	}
	for _, start := range merged {
		if s.Events != nil {
			s.Events.Publish(eventTypeTelemetry, TelemetryEvent{Start: start, End: start.Add(telemetry.WindowDuration), Scopes: len(s.Telemetry.Latest())})
		}
	}
	return nil
}

func (s *Server) telemetryLatest(w http.ResponseWriter, _ *http.Request) {
	if s.Telemetry == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "this node has no telemetry store")
		return
	}
	if err := s.flushLocalTelemetry(); err != nil {
		fail(w, err)
		return
	}
	windows := s.Telemetry.Latest()
	if windows == nil {
		windows = []telemetry.ScopeWindow{}
	}
	writeJSON(w, http.StatusOK, LatestTelemetry{Windows: windows})
}

func validScope(v string) bool {
	return v == "fleet" || strings.HasPrefix(v, "cluster:") && len(v) > len("cluster:") ||
		strings.HasPrefix(v, "proxy:") && len(v) > len("proxy:") || strings.HasPrefix(v, "bucket:") && len(v) > len("bucket:")
}

func validSeries(v string) bool {
	switch v {
	case telemetry.SeriesClientTotal, telemetry.SeriesUpstreamTTFB, telemetry.SeriesUpstreamTotal, telemetry.SeriesProxyOverhead:
		return true
	case telemetry.SeriesRequestsPerSecond, telemetry.SeriesBytesInPerSecond, telemetry.SeriesBytesOutPerSecond,
		telemetry.SeriesErrors0PerSecond, telemetry.SeriesErrors4xxPerSecond, telemetry.SeriesErrors5xxPerSecond, telemetry.SeriesNotFoundPerSecond, telemetry.SeriesStatusPerSecond:
		return true
	}
	return false
}

func validOp(v telemetry.OpClass) bool {
	switch v {
	case telemetry.OpAll, telemetry.OpRead, telemetry.OpWrite, telemetry.OpList, telemetry.OpDelete, telemetry.OpMultipart, telemetry.OpOther:
		return true
	}
	return false
}

func parseTelemetryTime(name, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, bad("%s %q: want RFC3339", name, value)
	}
	return t, nil
}

func (s *Server) telemetrySeries(w http.ResponseWriter, r *http.Request) {
	if s.Telemetry == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "this node has no telemetry store")
		return
	}
	q := r.URL.Query()
	scope, series, op := q.Get("scope"), q.Get("series"), telemetry.OpClass(q.Get("op"))
	if op == "" {
		op = telemetry.OpAll
	}
	if !validScope(scope) {
		fail(w, bad("scope %q: want fleet, cluster:<name>, or proxy:<id>", scope))
		return
	}
	if !validSeries(series) {
		fail(w, bad("series %q: want a latency series or requests/bytes/error rate series", series))
		return
	}
	if !validOp(op) {
		fail(w, bad("op %q: want all, read, write, list, delete, multipart, or other", op))
		return
	}
	from, err := parseTelemetryTime("from", q.Get("from"))
	if err != nil {
		fail(w, err)
		return
	}
	to, err := parseTelemetryTime("to", q.Get("to"))
	if err != nil {
		fail(w, err)
		return
	}
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		fail(w, bad("from must not be after to"))
		return
	}
	if err := s.flushLocalTelemetry(); err != nil {
		fail(w, err)
		return
	}
	var points []TelemetryPoint
	if series == telemetry.SeriesClientTotal || series == telemetry.SeriesUpstreamTTFB ||
		series == telemetry.SeriesUpstreamTotal || series == telemetry.SeriesProxyOverhead {
		for _, point := range s.Telemetry.Series(scope, series, op, from, to) {
			points = append(points, TelemetryPoint{Start: point.Start, End: point.End, Series: point.Series,
				Op: point.Op, Count: point.Count, P50: point.P50, P90: point.P90, P99: point.P99,
				P999: point.P999, Max: point.Max})
		}
	} else {
		for _, point := range s.Telemetry.CounterSeries(scope, series, op, from, to) {
			value := point.Value
			points = append(points, TelemetryPoint{Start: point.Start, End: point.End, Series: point.Series,
				Op: point.Op, Value: &value})
		}
	}
	if points == nil {
		points = []TelemetryPoint{}
	}
	writeJSON(w, http.StatusOK, TelemetrySeries{Scope: scope, Series: series, Op: op, Points: points})
}
