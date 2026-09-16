package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/s3/xmlrw"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Handler is the one pipeline. All fields are set once at startup.
type Handler struct {
	// Passthrough mode: every request goes to Cluster with Host and signature untouched.
	Cluster *upstream.Cluster

	// Resign mode (ADR-0001): the client signature is verified against Store, the request is
	// routed by its placement in Dir to one of Clusters under that cluster's backend bucket name,
	// and re-signed with the cluster's credentials. Rewrite (off by kill_switches.xml_rewrite_disable) rewrites the
	// response echoes of backend names, endpoints, and uploadIds (ADR-0006).
	Mode      Mode
	Store     sigv4.CredentialStore
	Clusters  *upstream.Set
	Dir       directory.Directory
	Rewrite   bool
	ClockSkew time.Duration

	Domains         s3.Domains
	Metrics         *telemetry.Metrics
	Access          *telemetry.AccessLogger
	Slow            *telemetry.SlowRing
	IdleTimeout     time.Duration // data ops: abort when no bytes move for this long
	MetadataTimeout time.Duration // metadata ops: total deadline
	Via             string        // e.g. "1.1 shunt/0.1.0"
	Log             *slog.Logger  // alerts (compensation, rewrite overflow, directory failures); nil means none

	pool    *bufPool   // body copy buffers
	scratch *bufPool   // rewritten-response scratch (ADR-0006)
	xw      *sync.Pool // *xmlrw.Writer
}

// New wires a Handler. copyBufferBytes is the pooled buffer size (config proxy.copy_buffer_bytes).
func New(h Handler, copyBufferBytes int) *Handler {
	h.pool = newBufPool(copyBufferBytes)
	h.scratch = newBufPool(xmlrw.MaxText)
	h.xw = &sync.Pool{New: func() any { return xmlrw.New(nil, nil, xmlrw.MaxText) }}
	if h.Via == "" {
		h.Via = "1.1 shunt"
	}
	if h.ClockSkew == 0 {
		h.ClockSkew = 15 * time.Minute
	}
	return &h
}

// outcome is filled in as the request progresses and recorded exactly once by finish().
type outcome struct {
	start       time.Time
	info        s3.RequestInfo
	rid         string
	upstream    string
	status      int
	bytesIn     int64
	bytesOut    int64
	err         string
	tm          *timings  // upstream connect and first-byte times, written by httptrace callbacks
	tLast       time.Time // last upstream body byte
	tParsed     time.Time
	upstreamID  string
	recorded    bool
	tenant      string
	accessKey   string
	cluster     string // "none" until a cluster is chosen
	clusterType string
	backend     string // backend bucket name (resign mode)
}

// prepared is a request ready to send: the cluster, a builder that produces the upstream request
// for one endpoint (called again when an endpoint refuses the connection), and what the response
// path needs to know.
type prepared struct {
	cl      *upstream.Cluster
	backend string
	// other is the placement's second cluster, set when the route needs it: a read that falls
	// back, a delete that goes to both, or a listing that merges (docs/DESIGN.md §2.5).
	other        *upstream.Cluster
	otherBackend string
	route        migrate.Route
	bucketKey    string // "<tenant>/<bucket>", the label on the migration metrics
	build        func(ctx context.Context, cl *upstream.Cluster, backend, endpoint string) (*http.Request, error)
	plan         *bodyPlan            // resign: the payload decision
	ed           *editor              // resign with Rewrite: response echo rewriting
	placement    *directory.Placement // resign: the placement the request was routed by
}

// timings holds what the httptrace callbacks measure. They run on the transport's goroutines, so
// every field is guarded: finish() can read them while a callback is still firing.
type timings struct {
	mu      sync.Mutex
	connect time.Duration
	ttfb    time.Duration
	first   time.Time // upstream first response byte
}

func (t *timings) set(connect, ttfb time.Duration, first time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if connect > 0 {
		t.connect = connect
	}
	if ttfb > 0 {
		t.ttfb = ttfb
	}
	if !first.IsZero() {
		t.first = first
	}
}

func (t *timings) get() (connect, ttfb time.Duration, first time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connect, t.ttfb, t.first
}

type buildError struct{ err error }

func (e *buildError) Error() string { return "build upstream request: " + e.err.Error() }
func (e *buildError) Unwrap() error { return e.err }

// ServeHTTP is the pipeline. It panics with http.ErrAbortHandler when a body was cut short,
// so net/http closes the client connection instead of finishing the response (ADR-0003).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o := &outcome{start: time.Now(), rid: telemetry.NewRequestID(), cluster: "none", clusterType: "none", tm: &timings{}}
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
	var inBody *progressReader
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
		inBody = &progressReader{r: r.Body, wd: wd, rc: rc, idle: h.IdleTimeout}
	}

	var p *prepared
	if h.Mode == ModeResign {
		var ok bool
		if p, ok = h.prepareResign(ctx, w, r, o, inBody); !ok {
			return // shunt answered: auth failure, synthesized response, or a refusal
		}
	} else {
		p = h.preparePassthrough(r, o, inBody)
	}

	// A delete during a migration goes to the source first and the primary second (ADR-0004 race 1):
	// a mover that copies the object in between then finds the source already gone when it
	// re-HEADs it, and takes its copy back out. The other order leaves that copy on the primary.
	source := ""
	if h.Mode == ModeResign && p.route.Both && p.other != nil {
		source = h.deleteOnSource(ctx, o, p)
	}
	resp, err := h.roundTrip(ctx, o, p, p.cl, p.backend, inBody)
	if source != "" {
		h.countDualDelete(o, p, source, resp, err)
	}
	if inBody != nil {
		o.bytesIn = inBody.count()
	}
	if err != nil {
		var berr *buildError
		if errors.As(err, &berr) {
			o.status = http.StatusBadRequest
			o.err = berr.Error()
			writeError(w, s3.InvalidURI, o.status, r.URL.Path, o.rid)
			return
		}
		var derr s3.Error
		if p.plan != nil && p.plan.decoder != nil && errors.As(err, &derr) {
			// The decoder rejected the client's body (chunk signature, trailer, length): a client
			// error, answered as such. If every decoded byte had already gone upstream the backend
			// may have committed the object: ADR-0002 log-and-alert.
			reason := sigv4.ReasonChunkSignature
			if derr.Code == s3.BadDigest || derr.Code == s3.MalformedTrailerError {
				reason = sigv4.ReasonTrailer
			}
			h.Metrics.AuthFailures.WithLabelValues(string(reason)).Inc()
			if p.plan.decoder.DataRead() >= p.plan.decodedLen && p.plan.decodedLen > 0 {
				h.compensate("trailer", r, o, derr.Error())
			}
			o.status = derr.Status
			o.err = "auth: " + string(reason)
			writeAuthError(w, &sigv4.AuthError{Reason: reason, Err: derr}, r.URL.Path, o.rid)
			return
		}
		h.upstreamError(w, r, o, err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck // drained or aborted below
	h.relay(ctx, w, r, o, p, resp, wd, rc)
}

// preparePassthrough forwards with the same method, raw path and query, and Host preserved.
func (h *Handler) preparePassthrough(r *http.Request, o *outcome, inBody *progressReader) *prepared {
	o.cluster, o.clusterType = h.Cluster.Name, h.Cluster.Type
	var body io.Reader
	if inBody != nil {
		body = inBody
	}
	build := func(ctx context.Context, _ *upstream.Cluster, _, endpoint string) (*http.Request, error) {
		out, err := http.NewRequestWithContext(ctx, r.Method, h.Cluster.Scheme+"://"+endpoint+r.URL.RequestURI(), body) //nolint:gosec // G704: forwarding to a configured cluster endpoint is the proxy's purpose; scheme and host come from config, only path and query from the client
		if err != nil {
			return nil, err
		}
		out.Host = r.Host
		out.ContentLength = r.ContentLength // Transport writes Content-Length from this, never from the header map
		copyHeaders(out.Header, r.Header)
		if _, ok := r.Header["User-Agent"]; !ok {
			out.Header.Set("User-Agent", "") // suppress the Transport's default UA; the client sent none
		}
		appendVia(out.Header, h.Via)
		out.Header.Set(telemetry.HeaderRequestID, o.rid)
		return out, nil
	}
	return &prepared{cl: h.Cluster, build: build}
}

// roundTrip sends the request, moving to the cluster's next endpoint when one refuses the
// connection before any request byte was sent (POC-3: round-robin plus skip-on-connect-error).
func (h *Handler) roundTrip(ctx context.Context, o *outcome, p *prepared, cl *upstream.Cluster, backend string, inBody *progressReader) (*http.Response, error) {
	// One trace struct per request: the only per-request allocation beyond net/http's own,
	// justified because connect time and TTFB are catalog signals (docs/DESIGN.md §2.7).
	var tConnect, tWrote time.Time
	trace := &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) { tConnect = time.Now() },
		ConnectDone: func(_, _ string, _ error) {
			if !tConnect.IsZero() {
				o.tm.set(time.Since(tConnect), 0, time.Time{})
			}
		},
		WroteHeaders: func() { tWrote = time.Now() },
		GotFirstResponseByte: func() {
			first := time.Now()
			ttfb := time.Duration(0)
			if !tWrote.IsZero() {
				ttfb = first.Sub(tWrote)
			}
			o.tm.set(0, ttfb, first)
		},
	}
	tctx := httptrace.WithClientTrace(ctx, trace)
	for attempt := 1; ; attempt++ {
		endpoint := cl.Next()
		o.upstream = endpoint
		out, err := p.build(tctx, cl, backend, endpoint)
		if err != nil {
			return nil, &buildError{err}
		}
		resp, err := cl.Transport.RoundTrip(out)
		if err != nil && attempt < len(cl.Endpoints) && upstream.IsConnectError(err) && (inBody == nil || inBody.count() == 0) {
			if h.Log != nil {
				h.Log.Warn("upstream endpoint refused the connection; trying the next endpoint",
					"request_id", o.rid, "cluster", cl.Name, "endpoint", endpoint, "err", err.Error())
			}
			continue
		}
		return resp, err
	}
}

// relay sends the upstream response to the client.
func (h *Handler) relay(ctx context.Context, w http.ResponseWriter, r *http.Request, o *outcome, p *prepared, resp *http.Response, wd *watchdog, rc *http.ResponseController) {
	if p.plan != nil && p.plan.sha != nil && resp.StatusCode < 300 {
		if got := hexSum(p.plan.sha); got != p.plan.expectHex {
			h.compensate("sha256", r, o, "x-amz-content-sha256 "+p.plan.expectHex+" does not match body "+got)
		}
	}
	o.upstreamID = resp.Header.Get("X-Amz-Request-Id")
	if h.Mode == ModeResign {
		if isRedirect(resp.StatusCode) {
			h.failRedirect(w, r, o, p, resp)
			return
		}
		if !h.afterDeleteBucket(ctx, w, r, o, p, resp) {
			return
		}
		// The primary does not have this object yet; the source still might (§2.5).
		if p.route.Fallback && resp.StatusCode == http.StatusNotFound && p.other != nil {
			if alt := h.fallbackRead(ctx, r, o, p, resp); alt != nil {
				resp = alt
				defer resp.Body.Close() //nolint:errcheck // relayed or aborted below
			}
		}
	}

	copyHeaders(w.Header(), resp.Header)
	appendVia(w.Header(), h.Via)
	if _, ok := resp.Header["Content-Type"]; !ok {
		// Transparency: net/http sniffs a Content-Type onto any body that lacks one. The backend
		// sent none, so the client must see none. A nil value disables the sniffing.
		w.Header()["Content-Type"] = nil
	}
	if p.ed != nil {
		if loc := resp.Header.Get("Location"); loc != "" {
			w.Header().Set("Location", p.ed.locationHeader(loc))
		}
	}
	o.status = resp.StatusCode

	// Stream the body. dst hides ReaderFrom so io.CopyBuffer goes through our pooled buffer.
	buf := h.pool.get()
	defer h.pool.put(buf)
	dst := &deadlineWriter{w: w, rc: rc, idle: h.IdleTimeout}
	src := &progressReader{r: resp.Body, wd: wd}

	if p.ed != nil && h.shouldRewrite(r, o, resp) {
		h.relayRewritten(w, o, p, resp, dst, src, *buf)
		return
	}

	w.WriteHeader(resp.StatusCode)
	_, copyErr := io.CopyBuffer(dst, src, *buf)
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

// relayRewritten streams the body through the XML rewriter into a scratch buffer; a body that
// completes within the scratch goes out with an exact Content-Length, a larger one spills to
// chunked (ADR-0006).
func (h *Handler) relayRewritten(w http.ResponseWriter, o *outcome, p *prepared, resp *http.Response, dst *deadlineWriter, src *progressReader, buf []byte) {
	w.Header().Del("Content-Length")
	scratch := h.scratch.get()
	sp := &spill{w: w, dst: dst, status: resp.StatusCode, buf: *scratch}
	xw := h.xw.Get().(*xmlrw.Writer) //nolint:errcheck // the pool holds only *xmlrw.Writer
	xw.Reset(sp, p.ed)
	_, copyErr := io.CopyBuffer(xw, src, buf)
	if copyErr == nil {
		copyErr = xw.Flush()
	}
	if copyErr == nil {
		copyErr = sp.finish()
	}
	overflows := xw.Overflows
	xw.Reset(nil, nil)
	h.xw.Put(xw)
	h.scratch.put(scratch)
	o.bytesOut = dst.n
	o.tLast = time.Now()
	if overflows > 0 && h.Log != nil {
		h.Log.Warn("response rewrite forwarded elements verbatim (text over the cap, or markup inside); a backend name may have reached the client",
			"request_id", o.rid, "op", o.info.Op.String(), "cluster", o.cluster, "elements", overflows)
	}
	if copyErr != nil {
		o.err = "body: " + copyErr.Error()
		if sp.sent && resp.ContentLength >= 0 && src.count() == resp.ContentLength {
			return // the whole upstream body was read; the client stopped reading
		}
		panic(http.ErrAbortHandler)
	}
}

// compensate records a late payload-check failure (ADR-0002). The POC only logs and counts;
// the compensating delete is P2 work.
func (h *Handler) compensate(reason string, r *http.Request, o *outcome, detail string) {
	h.Metrics.Compensation.WithLabelValues(reason, "logged").Inc()
	if h.Log != nil {
		h.Log.Error("compensation needed: upstream write completed with a payload mismatch (ADR-0002, log-and-alert only)",
			"reason", reason, "request_id", o.rid, "method", r.Method, "bucket", o.info.Bucket, "op", o.info.Op.String(),
			"upstream", o.upstream, "cluster", o.cluster, "backend_bucket", o.backend, "detail", detail)
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
	writeErrorMessage(w, code, status, "", resource, rid)
}

func writeErrorMessage(w http.ResponseWriter, code s3.Code, status int, message, resource, rid string) {
	body := s3.Render(code, message, resource, rid, "")
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
	connect, ttfb, tFirst := o.tm.get()
	op := o.info.Op.String()
	h.Metrics.RequestsTotal.WithLabelValues(op, telemetry.StatusClass(o.status), o.cluster, o.clusterType).Inc()
	h.Metrics.RequestDuration.WithLabelValues(op).Observe(total.Seconds())
	if ttfb > 0 {
		h.Metrics.UpstreamTTFB.WithLabelValues(op, o.cluster).Observe(ttfb.Seconds())
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
		BytesIn: o.bytesIn, BytesOut: o.bytesOut, Duration: total, TTFB: ttfb,
		Upstream: o.upstream, Cluster: o.cluster, ClusterType: o.clusterType, Tenant: o.tenant, BackendBucket: o.backend,
		TLS: tlsVer, Error: o.err,
	})
	if h.Slow != nil && total >= h.Slow.Threshold() {
		e := telemetry.SlowEntry{
			At: o.start, RequestID: o.rid, UpstreamRequestID: o.upstreamID, Op: op, Method: r.Method,
			Bucket: o.info.Bucket, Status: o.status, BytesIn: o.bytesIn, BytesOut: o.bytesOut,
			Upstream: o.upstream, Parse: o.tParsed.Sub(o.start), UpstreamConnect: connect,
			UpstreamTTFB: ttfb, Total: total, Error: o.err,
		}
		if !tFirst.IsZero() && !o.tLast.IsZero() {
			e.UpstreamBody = o.tLast.Sub(tFirst)
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
