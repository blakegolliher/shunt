package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// fake starts an in-process S3 server (gofakes3, MIT, test-only) holding bucket with n objects under
// "a/" and n under "b/".
func fake(tb testing.TB, bucket string, n int) (*httptest.Server, *s3mem.Backend) {
	tb.Helper()
	be := s3mem.New()
	if err := be.CreateBucket(bucket); err != nil {
		tb.Fatal(err)
	}
	fill(tb, be, bucket, n)
	srv := httptest.NewServer(gofakes3.New(be).Server())
	tb.Cleanup(srv.Close)
	return srv, be
}

func fill(tb testing.TB, be *s3mem.Backend, bucket string, n int) {
	tb.Helper()
	for _, dir := range []string{"a", "b"} {
		for i := 0; i < n; i++ {
			if _, err := be.PutObject(bucket, fmt.Sprintf("%s/%05d", dir, i), nil, strings.NewReader("x"), 1, nil); err != nil {
				tb.Fatal(err)
			}
		}
	}
}

func newCleaner(tb testing.TB, url, bucket string) *cleaner {
	tb.Helper()
	c, err := newClient(url, "", "", "us-east-1", "ak", "sk", "", 4, false, false)
	if err != nil {
		tb.Fatal(err)
	}
	return &cleaner{c: c, bucket: bucket, page: 1000, workers: 4, errOut: func(f string, a ...any) { tb.Errorf(f, a...) }}
}

func count(tb testing.TB, cl *cleaner) int64 {
	tb.Helper()
	dry := *cl
	dry.dryRun = true
	t, err := dry.pass(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	return t.listed.Load()
}

func TestRunEmptiesBucket(t *testing.T) {
	for _, perKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("perKey=%v", perKey), func(t *testing.T) {
			srv, _ := fake(t, "bkt", 1250)
			cl := newCleaner(t, srv.URL, "bkt")
			cl.page, cl.perKey = 300, perKey
			var rounds []int64
			empty, err := cl.run(context.Background(), 5, func(_ int, tl *tally, _ time.Duration) {
				if tl.failed.Load() != 0 {
					t.Errorf("failed=%d", tl.failed.Load())
				}
				rounds = append(rounds, tl.deleted.Load())
			})
			if err != nil || !empty {
				t.Fatalf("empty=%v err=%v", empty, err)
			}
			if len(rounds) != 2 || rounds[0] != 2500 || rounds[1] != 0 {
				t.Fatalf("per-round deletes %v, want [2500 0]", rounds)
			}
		})
	}
}

func TestPrefixLeavesOtherKeys(t *testing.T) {
	srv, _ := fake(t, "bkt", 40)
	cl := newCleaner(t, srv.URL, "bkt")
	cl.prefix = "a/"
	if empty, err := cl.run(context.Background(), 3, func(int, *tally, time.Duration) {}); err != nil || !empty {
		t.Fatalf("empty=%v err=%v", empty, err)
	}
	cl.prefix = ""
	if n := count(t, cl); n != 40 {
		t.Fatalf("%d keys left, want the 40 under b/", n)
	}
}

func TestDryRunDeletesNothing(t *testing.T) {
	srv, _ := fake(t, "bkt", 30)
	cl := newCleaner(t, srv.URL, "bkt")
	cl.dryRun = true
	empty, err := cl.run(context.Background(), 5, func(int, *tally, time.Duration) {})
	if err != nil || empty {
		t.Fatalf("empty=%v err=%v", empty, err)
	}
	cl.dryRun = false
	if n := count(t, cl); n != 60 {
		t.Fatalf("%d keys, want 60", n)
	}
}

func TestMissingBucketIsAnError(t *testing.T) {
	srv, _ := fake(t, "bkt", 1)
	cl := newCleaner(t, srv.URL, "nope")
	if _, err := cl.run(context.Background(), 1, func(int, *tally, time.Duration) {}); err == nil {
		t.Fatal("want an error listing a missing bucket")
	}
}

func BenchmarkPass(b *testing.B) {
	srv, be := fake(b, "bkt", 500)
	cl := newCleaner(b, srv.URL, "bkt")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, err := cl.pass(context.Background())
		if err != nil || t.deleted.Load() != 1000 {
			b.Fatalf("deleted=%d err=%v", t.deleted.Load(), err)
		}
		b.StopTimer()
		fill(b, be, "bkt", 500)
		b.StartTimer()
	}
}
