package proxy

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// bufPool hands out fixed-size copy buffers. Pooled so a streaming request allocates no
// per-byte memory after warm-up; *[]byte avoids an interface allocation on Put.
type bufPool struct {
	size int
	p    sync.Pool
}

func newBufPool(size int) *bufPool {
	bp := &bufPool{size: size}
	bp.p.New = func() any { b := make([]byte, size); return &b }
	return bp
}

func (bp *bufPool) get() *[]byte  { return bp.p.Get().(*[]byte) } //nolint:errcheck // pool holds only *[]byte
func (bp *bufPool) put(b *[]byte) { bp.p.Put(b) }

// progressReader resets the idle watchdog on every successful read and counts bytes.
// Used on the upstream response body and on the client request body (as seen by the Transport).
// A request body is read by net/http's transport write loop, a different goroutine from the one
// that reads the count afterwards, so the counter is atomic.
type progressReader struct {
	r    io.Reader
	wd   *watchdog
	n    atomic.Int64
	rc   *http.ResponseController // set for the client request body: refresh the server read deadline
	idle time.Duration
	// ans, set on a mutation's response body, watches for the backend's whole answer (R3-03).
	ans *answerWatch
}

// answerWatch is what a mutation's response body showed of the backend's answer: whether it was
// read to its end, and, for an operation whose 200 carries its result in the body, the body's last
// bytes. Only the handler's goroutine reads a response body, so it needs no lock.
type answerWatch struct {
	eof        bool
	result     bool // the 200 carries the operation's result, or an error, in its body
	tail       [64]byte
	tailLen    int
	tailClosed bool
}

// record keeps the last bytes read.
func (a *answerWatch) record(b []byte) {
	if len(b) >= len(a.tail) {
		a.tailLen = copy(a.tail[:], b[len(b)-len(a.tail):])
		return
	}
	if keep := len(a.tail) - len(b); a.tailLen > keep {
		copy(a.tail[:], a.tail[a.tailLen-keep:a.tailLen])
		a.tailLen = keep
	}
	a.tailLen += copy(a.tail[a.tailLen:], b)
}

// resultClosings end a body that carries a complete result or error (AWS: CompleteMultipartUpload,
// CopyObject and UploadPartCopy can answer 200 before they have finished, then write either).
var resultClosings = [][]byte{[]byte("</CompleteMultipartUploadResult>"), []byte("</CopyObjectResult>"), []byte("</CopyPartResult>"), []byte("</Error>")}

// whole reports whether the backend's answer was read whole: to its end, and for a result body,
// through the close of its result or error element.
func (a *answerWatch) whole() bool {
	if !a.eof {
		return false
	}
	if !a.result {
		return true
	}
	tail := bytes.TrimRight(a.tail[:a.tailLen], " \t\r\n")
	for _, c := range resultClosings {
		if bytes.HasSuffix(tail, c) {
			return true
		}
	}
	return false
}

// count reports the bytes read so far.
func (p *progressReader) count() int64 { return p.n.Load() }

func (p *progressReader) Read(b []byte) (int, error) {
	if p.rc != nil {
		_ = p.rc.SetReadDeadline(time.Now().Add(p.idle)) // unsupported writers return an error we can ignore
	}
	n, err := p.r.Read(b)
	if n > 0 {
		p.n.Add(int64(n))
		if p.wd != nil {
			p.wd.kick()
		}
		if p.ans != nil && p.ans.result {
			p.ans.record(b[:n])
		}
	}
	if err == io.EOF && p.ans != nil {
		p.ans.eof = true
	}
	return n, err
}

// deadlineWriter refreshes the server write deadline before every write and counts bytes.
// It hides ReaderFrom so io.CopyBuffer uses our pooled buffer.
type deadlineWriter struct {
	w    io.Writer
	rc   *http.ResponseController
	idle time.Duration
	n    int64
}

func (d *deadlineWriter) Write(b []byte) (int, error) {
	if d.rc != nil {
		_ = d.rc.SetWriteDeadline(time.Now().Add(d.idle))
	}
	n, err := d.w.Write(b)
	d.n += int64(n)
	return n, err
}

// watchdog cancels the upstream request when no bytes move for idle. One timer per data
// request; kick() is a timer reset, no allocation.
type watchdog struct {
	t    *time.Timer
	idle time.Duration
}

func newWatchdog(idle time.Duration, cancel func()) *watchdog {
	return &watchdog{t: time.AfterFunc(idle, cancel), idle: idle}
}

func (w *watchdog) kick() { w.t.Reset(w.idle) }
func (w *watchdog) stop() { w.t.Stop() }
