package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// The two tests here exist to give `go test -race` something to find. Neither asserts anything
// about a response: their job is to hold open the windows in which the handler and net/http's
// transport touch the same request state at the same time.
//
// On a request that runs to completion there is a happens-before edge between them: RoundTrip
// returns only after the body has been written and the response read, so the handler's bookkeeping
// afterwards is ordered and the detector sees nothing. Two races lived through POC-1 and POC-2
// because the suite opened those windows only a handful of times per run, and `-race` reports a
// race only when it actually observes both accesses. Each test below opens its window about a
// hundred times per run. Both are cheap: no e2e stack, well under a second each.
//
// Reverting either fix makes the matching test fail immediately (docs/STATUS.md, POC-3).

// slowBody emits a request body in small pieces with a pause between them, so the transport's
// write loop is still reading it when the backend has already answered.
type slowBody struct {
	total, chunk, sent int
}

func (s *slowBody) Read(p []byte) (int, error) {
	if s.sent >= s.total {
		return 0, io.EOF
	}
	time.Sleep(200 * time.Microsecond)
	n := min(min(len(p), s.chunk), s.total-s.sent)
	for i := range p[:n] {
		p[i] = 'x'
	}
	s.sent += n
	return n, nil
}

// TestConcurrentAbortedRequestsAreRaceFree covers the request-body byte counter, which the
// transport's write loop updates on its own goroutine. The window opens when the transport
// abandons a request while its body is still being written: the backend answers before reading the
// body, or the client goes away mid-body. The write loop then keeps reading (bumping the counter)
// while the handler is already recording the outcome.
func TestConcurrentAbortedRequestsAreRaceFree(t *testing.T) {
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "reject") {
			// Answer without reading the body: the transport gives up on the write loop mid-body.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code></Error>`))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	r := newRig(t, backend, 2*time.Second)

	const workers, each = 16, 6
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := fresh() // no keep-alive: every request dials, so the trace hooks fire every time
			for j := range each {
				path, body := "/bkt/accept", &slowBody{total: 2 << 20, chunk: 32 << 10}
				if (i+j)%2 == 0 {
					path = "/bkt/reject"
				}
				req, err := http.NewRequest(http.MethodPut, r.front.URL+path, body)
				if err != nil {
					t.Error(err)
					return
				}
				req.ContentLength = int64(body.total)
				ctx := context.Background()
				if (i+j)%5 == 0 {
					// The client disappears mid-body, so the handler finishes while the transport
					// is still working.
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 15*time.Millisecond)
					defer cancel()
				}
				resp, err := client.Do(req.WithContext(ctx))
				if err != nil {
					continue // a canceled or rejected request is the point, not a failure
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()
}

// TestLateResponseHeadersAreRaceFree covers the httptrace timings. They are written by the
// connection's read loop, a goroutine the transport keeps per connection, not by the handler's.
// GotFirstResponseByte fires when the response headers arrive, and that can be after the handler
// has already given up on the request and recorded its outcome: the client went away, or the
// request's own context ended, while the backend was still deciding what to answer. The original
// two failures were exactly this, in the POC-2 tests that reject a body mid-stream.
//
// Here the backend holds every response back for longer than the client waits, so each request's
// handler finishes first and the headers land afterwards, on a goroutine that is still writing the
// timings the handler just read.
func TestLateResponseHeadersAreRaceFree(t *testing.T) {
	r := newRig(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Read the body first so the transport's write loop is done with it: this test is about the
		// response side only.
		_, _ = io.Copy(io.Discard, req.Body)
		select {
		case <-time.After(30 * time.Millisecond):
		case <-req.Context().Done():
		}
		w.Header().Set("X-Amz-Request-Id", "late")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late body"))
	}), 2*time.Second)

	const workers, each = 12, 8
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := fresh()
			for range each {
				// The client waits far less than the backend takes, so the handler is done long
				// before the response headers arrive on the connection's read loop.
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.front.URL+"/bkt/key", http.NoBody)
				if err != nil {
					cancel()
					t.Error(err)
					return
				}
				resp, err := client.Do(req)
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				cancel()
			}
		}()
	}
	wg.Wait()
}
