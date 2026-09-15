package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Handler is the one pipeline. All fields are set once at startup.
type Handler struct {
	Cluster         *upstream.Cluster
	Domains         s3.Domains
	Metrics         *telemetry.Metrics
	Access          *telemetry.AccessLogger
	Slow            *telemetry.SlowRing
	IdleTimeout     time.Duration // data ops: abort when no bytes move for this long
	MetadataTimeout time.Duration // metadata ops: total deadline
	Via             string        // e.g. "1.1 shunt/0.1.0"

	pool *bufPool
}

// New wires a Handler. copyBufferBytes is the pooled buffer size (config proxy.copy_buffer_bytes).
func New(h Handler, copyBufferBytes int) *Handler {
	h.pool = newBufPool(copyBufferBytes)
	if h.Via == "" {
		h.Via = "1.1 shunt"
	}
	return &h
}

// outcome is filled in as the request progresses and recorded exactly once by finish().
type outcome struct {
	start      time.Time
	info       s3.RequestInfo
	rid        string
	upstream   string
	status     int
	bytesIn    int64
	bytesOut   int64
	err        string
	connect    time.Duration
	ttfb       time.Duration
	tFirst     time.Time // upstream first response byte
	tLast      time.Time // last upstream body byte
	tParsed    time.Time
	upstreamID string
	recorded   bool
}

// ServeHTTP is the pipeline. It panics with http.ErrAbortHandler when a body was cut short,
// so net/http closes the client connection instead of finishing the response (ADR-0003).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o := &outcome{start: time.Now(), rid: telemetry.NewRequestID()}
	w.Header().Set(telemetry.HeaderRequestID, o.rid)
	o.info = s3.Parse(r, h.Domains)
	o.tParsed = time.Now()
	op := o.info.Op.String()
	h.Metrics.Inflight.WithLabelValues(op).Inc()
	defer h.Metrics.Inflight.WithLabelValues(op).Dec()
	defer h.finish(r, o)

	// Deadline class (docs/DESIGN.md §2.8): metadata ops get a total deadline, data ops an
	// idle-progress watchdog that cancels the upstream request when nothing moves.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var wd *watchdog
	if o.info.Op.IsData() {
		wd = newWatchdog(h.IdleTimeout, cancel)
		defer wd.stop()
	} else {
		var c2 context.CancelFunc
		ctx, c2 = context.WithTimeout(ctx, h.MetadataTimeout)
		defer c2()
	}

	rc := http.NewResponseController(w)
	endpoint := h.Cluster.Next()
	o.upstream = endpoint

	// Upstream request: same method, same raw path and query, Host preserved (passthrough).
	var body io.Reader
	var inBody *progressReader
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
		inBody = &progressReader{r: r.Body, wd: wd, rc: rc, idle: h.IdleTimeout}
		body = inBody
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, h.Cluster.Scheme+"://"+endpoint+r.URL.RequestURI(), body) //nolint:gosec // G704: forwarding to a configured cluster endpoint is the proxy's purpose; scheme and host come from config, only path and query from the client
	if err != nil {
		o.status = http.StatusBadRequest
		o.err = "build upstream request: " + err.Error()
		writeError(w, s3.InvalidURI, o.status, r.URL.Path, o.rid)
		return
	}
	out.Host = r.Host
	out.ContentLength = r.ContentLength // Transport writes Content-Length from this, never from the header map
	copyHeaders(out.Header, r.Header)
	if _, ok := r.Header["User-Agent"]; !ok {
		out.Header.Set("User-Agent", "") // suppress the Transport's default UA; the client sent none
	}
	appendVia(out.Header, h.Via)
	out.Header.Set(telemetry.HeaderRequestID, o.rid)

	// One trace struct per request: the only per-request allocation beyond net/http's own,
	// justified because connect time and TTFB are catalog signals (docs/DESIGN.md §2.7).
	var tConnect, tWrote time.Time
	trace := &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) { tConnect = time.Now() },
		ConnectDone: func(_, _ string, _ error) {
			if !tConnect.IsZero() {
				o.connect = time.Since(tConnect)
			}
		},
		WroteHeaders: func() { tWrote = time.Now() },
		GotFirstResponseByte: func() {
			o.tFirst = time.Now()
			if !tWrote.IsZero() {
				o.ttfb = o.tFirst.Sub(tWrote)
			}
		},
	}
	out = out.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := h.Cluster.Transport.RoundTrip(out)
	if inBody != nil {
		o.bytesIn = inBody.n
	}
	if err != nil {
		h.upstreamError(w, r, o, err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck // drained or aborted below

	o.upstreamID = resp.Header.Get("X-Amz-Request-Id")
	copyHeaders(w.Header(), resp.Header)
	appendVia(w.Header(), h.Via)
	if _, ok := resp.Header["Content-Type"]; !ok {
		// Transparency: net/http sniffs a Content-Type onto any body that lacks one. The backend
		// sent none, so the client must see none. A nil value disables the sniffing.
		w.Header()["Content-Type"] = nil
	}
	w.WriteHeader(resp.StatusCode)
	o.status = resp.StatusCode

	// Stream the body. dst hides ReaderFrom so io.CopyBuffer goes through our pooled buffer.
	buf := h.pool.get()
	dst := &deadlineWriter{w: w, rc: rc, idle: h.IdleTimeout}
	src := &progressReader{r: resp.Body, wd: wd}
	_, copyErr := io.CopyBuffer(dst, src, *buf)
	h.pool.put(buf)
	o.bytesOut = dst.n
	o.tLast = time.Now()
	if copyErr != nil {
		// Upstream died or the client stopped reading: never let net/http finish the response
		// as if it were complete (ADR-0003). finish() runs in the deferred chain.
		o.err = "body: " + copyErr.Error()
		if resp.ContentLength >= 0 && dst.n == resp.ContentLength {
			return // client-side write error after a complete body; nothing to hide
		}
		panic(http.ErrAbortHandler)
	}
}

// upstreamError answers when no upstream response was produced: a timeout is 504, everything
// else 502, both with an S3 error body. If part of the client body was consumed, net/http
// closes the client connection after the handler returns.
func (h *Handler) upstreamError(w http.ResponseWriter, r *http.Request, o *outcome, err error) {
	o.err = "upstream: " + err.Error()
	code, status := s3.InternalError, http.StatusBadGateway
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		code, status = s3.RequestTimeout, http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		// The client went away; no response can be delivered.
		o.status = 0
		return
	}
	if errors.Is(err, context.Canceled) {
		code, status = s3.RequestTimeout, http.StatusGatewayTimeout // our idle watchdog fired
	}
	o.status = status
	writeError(w, code, status, r.URL.Path, o.rid)
}

func writeError(w http.ResponseWriter, code s3.Code, status int, resource, rid string) {
	body := s3.Render(code, "", resource, rid, "")
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body) //nolint:gosec // G705: body is XML produced by s3.Render, which escapes the resource path
}

// finish records metrics, the access log line, and the slow ring entry exactly once.
func (h *Handler) finish(r *http.Request, o *outcome) {
	if o.recorded {
		return
	}
	o.recorded = true
	total := time.Since(o.start)
	op := o.info.Op.String()
	h.Metrics.RequestsTotal.WithLabelValues(op, telemetry.StatusClass(o.status)).Inc()
	h.Metrics.RequestDuration.WithLabelValues(op).Observe(total.Seconds())
	if o.ttfb > 0 {
		h.Metrics.UpstreamTTFB.WithLabelValues(op, h.Cluster.Name).Observe(o.ttfb.Seconds())
	}
	if o.bytesIn > 0 {
		h.Metrics.BytesIn.WithLabelValues(op).Add(float64(o.bytesIn))
	}
	if o.bytesOut > 0 {
		h.Metrics.BytesOut.WithLabelValues(op).Add(float64(o.bytesOut))
	}
	tlsVer := ""
	if r.TLS != nil {
		tlsVer = tlsVersion(r.TLS.Version)
	}
	h.Access.Log(&telemetry.Access{
		RequestID: o.rid, UpstreamRequestID: o.upstreamID, Client: r.RemoteAddr, Method: r.Method,
		Host: r.Host, Style: o.info.Style.String(), Bucket: o.info.Bucket, Op: op, Status: o.status,
		BytesIn: o.bytesIn, BytesOut: o.bytesOut, Duration: total, TTFB: o.ttfb,
		Upstream: o.upstream, Cluster: h.Cluster.Name, TLS: tlsVer, Error: o.err,
	})
	if h.Slow != nil && total >= h.Slow.Threshold() {
		e := telemetry.SlowEntry{
			At: o.start, RequestID: o.rid, UpstreamRequestID: o.upstreamID, Op: op, Method: r.Method,
			Bucket: o.info.Bucket, Status: o.status, BytesIn: o.bytesIn, BytesOut: o.bytesOut,
			Upstream: o.upstream, Parse: o.tParsed.Sub(o.start), UpstreamConnect: o.connect,
			UpstreamTTFB: o.ttfb, Total: total, Error: o.err,
		}
		if !o.tFirst.IsZero() && !o.tLast.IsZero() {
			e.UpstreamBody = o.tLast.Sub(o.tFirst)
		}
		h.Slow.Record(&e)
	}
}

func tlsVersion(v uint16) string {
	switch v {
	case 0x0303:
		return "1.2"
	case 0x0304:
		return "1.3"
	}
	return strconv.Itoa(int(v))
}
