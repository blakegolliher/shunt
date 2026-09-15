package telemetry

import (
	"io"
	"log/slog"
	"time"
)

// Access is one access-log record. The field set is fixed and documented in
// docs/reference/access-log.md; the object key is never included.
type Access struct {
	RequestID         string
	UpstreamRequestID string
	Client            string // client IP:port
	Method            string
	Host              string
	Style             string
	Bucket            string
	Op                string
	Status            int
	BytesIn           int64
	BytesOut          int64
	Duration          time.Duration
	TTFB              time.Duration // upstream time to first header byte; 0 if none
	Upstream          string        // endpoint host:port
	Cluster           string
	TLS               string // e.g. "1.3", "" for plaintext
	Error             string // "" on success
}

// AccessLogger writes JSON lines.
type AccessLogger struct {
	l *slog.Logger
}

// NewAccessLogger writes one JSON object per line to w. A nil w disables logging.
func NewAccessLogger(w io.Writer) *AccessLogger {
	if w == nil {
		return &AccessLogger{}
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey || a.Key == slog.MessageKey {
				return slog.Attr{} // drop level and msg; every record is an access record
			}
			if a.Key == slog.TimeKey {
				a.Key = "ts"
			}
			return a
		},
	})
	return &AccessLogger{l: slog.New(h)}
}

// Log emits the record. Attribute order is the documented schema order.
func (a *AccessLogger) Log(r *Access) {
	if a.l == nil {
		return
	}
	a.l.LogAttrs(nil, slog.LevelInfo, "", //nolint:staticcheck // nil ctx is documented as allowed by slog
		slog.String("request_id", r.RequestID),
		slog.String("upstream_request_id", r.UpstreamRequestID),
		slog.String("client", r.Client),
		slog.String("method", r.Method),
		slog.String("host", r.Host),
		slog.String("style", r.Style),
		slog.String("bucket", r.Bucket),
		slog.String("op", r.Op),
		slog.Int("status", r.Status),
		slog.Int64("bytes_in", r.BytesIn),
		slog.Int64("bytes_out", r.BytesOut),
		slog.Float64("duration_ms", ms(r.Duration)),
		slog.Float64("ttfb_ms", ms(r.TTFB)),
		slog.String("upstream", r.Upstream),
		slog.String("cluster", r.Cluster),
		slog.String("tls", r.TLS),
		slog.String("error", r.Error),
	)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
