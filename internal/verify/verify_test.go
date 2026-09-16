package verify

import (
	"context"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/blakegolliher/shunt/internal/sigv4"
)

// backend is an in-process S3 endpoint with bucket b; wrap, if set, sees every request first.
func backend(t *testing.T, wrap func(w http.ResponseWriter, r *http.Request, next http.Handler)) *Client {
	t.Helper()
	be := s3mem.New()
	if err := be.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	fake := gofakes3.New(be, gofakes3.WithTimeSkewLimit(0)).Server()
	h := fake
	if wrap != nil {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { wrap(w, r, fake) })
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &Client{Endpoint: srv.URL, Bucket: "b", Creds: sigv4.Credentials{AccessKey: "AK", Secret: "SK"}}
}

func TestCleanRunHasNoErrors(t *testing.T) {
	c := backend(t, nil)
	var progress strings.Builder
	rep := Run(context.Background(), c, Options{Workers: 4, Keys: 20, Duration: 400 * time.Millisecond, Interval: 100 * time.Millisecond, Progress: &progress, Cleanup: true, Seed: 7})
	if rep.Errors != 0 || rep.Ops < 50 || rep.Puts == 0 || rep.Gets == 0 || rep.Deletes == 0 {
		t.Fatalf("clean run: %+v", rep)
	}
	if rep.Seed != 7 || !strings.HasPrefix(rep.Prefix, "verify/") || rep.WriteSides["unrouted"] == 0 {
		t.Fatalf("report fields: %+v", rep)
	}
	if !strings.Contains(progress.String(), "verify: ") || !strings.Contains(progress.String(), "0 errors") {
		t.Fatalf("progress: %q", progress.String())
	}
	if rep.ReadBack == 0 || rep.ReadBack > rep.Present {
		t.Fatalf("read back %d of %d present keys", rep.ReadBack, rep.Present)
	}
	if rep.CleanupErrors != 0 {
		t.Fatalf("cleanup: %d errors", rep.CleanupErrors)
	}
}

// A backend that acknowledges writes it did not keep is caught by the model.
func TestLostWritesAreErrors(t *testing.T) {
	var n atomic.Int64
	c := backend(t, func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		if r.Method == http.MethodPut && n.Add(1)%4 == 0 {
			w.WriteHeader(http.StatusOK) // claims success, stores nothing
			return
		}
		next.ServeHTTP(w, r)
	})
	rep := Run(context.Background(), c, Options{Workers: 4, Keys: 10, Duration: 400 * time.Millisecond})
	if rep.Errors == 0 || len(rep.ErrorSamples) == 0 {
		t.Fatalf("lost writes went unnoticed: %+v", rep)
	}
}

// With DebugRoute, writes and reads are tallied by the X-Shunt-Route the endpoint answers with.
func TestRouteTally(t *testing.T) {
	c := backend(t, func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		if r.Header.Get("X-Shunt-Debug") == "1" {
			h := fnv.New32a()
			_, _ = h.Write([]byte(r.URL.Path))
			if h.Sum32()%2 == 0 {
				w.Header().Set("X-Shunt-Route", "primary vast02")
			} else {
				w.Header().Set("X-Shunt-Route", "source vast01")
			}
		}
		next.ServeHTTP(w, r)
	})
	c.DebugRoute = true
	rep := Run(context.Background(), c, Options{Workers: 4, Keys: 40, Duration: 400 * time.Millisecond})
	if rep.Errors != 0 || rep.WriteSides["primary"] == 0 || rep.WriteSides["source"] == 0 || rep.Writes["primary vast02"] == 0 {
		t.Fatalf("route tally: %+v", rep)
	}
	if s := Share(rep.WriteSides); !strings.Contains(s, "primary ") || !strings.Contains(s, "%") {
		t.Fatalf("share: %s", s)
	}
}

func BenchmarkRunOps(b *testing.B) {
	be := s3mem.New()
	_ = be.CreateBucket("b")
	srv := httptest.NewServer(gofakes3.New(be, gofakes3.WithTimeSkewLimit(0)).Server())
	defer srv.Close()
	c := &Client{Endpoint: srv.URL, Bucket: "b", Creds: sigv4.Credentials{AccessKey: "AK", Secret: "SK"}}
	for b.Loop() {
		if _, err := c.Do(context.Background(), http.MethodPut, "k", []byte("v"), nil); err != nil {
			b.Fatal(err)
		}
	}
}
