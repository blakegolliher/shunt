package proxy

import (
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
