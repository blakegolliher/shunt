package proxy

// A merged listing's lookahead (third review, R3-10): after a page is filled, each side is peeked
// once to learn whether the listing is truncated. A peek that fails says nothing about that, and
// must never read as the end of the listing.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

// flakyContinuation fronts a fake so that, while fail is set, every listing page after the first
// (a continuation) answers 503.
func flakyContinuation(t *testing.T, m *mixedRig, name string, f *fakeS3) *atomic.Bool {
	t.Helper()
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() && r.URL.Query().Get("list-type") == "2" && r.URL.Query().Get("continuation-token") != "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code></Error>`))
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	cl := m.clusters[name]
	cl.Endpoints = []string{strings.TrimPrefix(srv.URL, "http://")}
	if err := m.dir.PutCluster(context.Background(), name, cl, "", "test"); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	return &fail
}

// listAll pages a merged listing to its end and returns every key, or the first failed page.
func listAll(t *testing.T, m *mixedRig, query string) ([]string, int) {
	t.Helper()
	var keys []string
	token := ""
	for range 20 {
		target := "/data?list-type=2&max-keys=1000" + query
		if token != "" {
			target += "&continuation-token=" + token
		}
		r := m.acme(t, "GET", target, nil)
		if r.StatusCode != http.StatusOK {
			return keys, r.StatusCode
		}
		keys = append(keys, listKeys(t, r.body, "Key")...)
		if !strings.Contains(string(r.body), "<IsTruncated>true</IsTruncated>") {
			return keys, http.StatusOK
		}
		token = between(r.body, "<NextContinuationToken>", "</NextContinuationToken>")
	}
	t.Fatal("the listing never ended")
	return nil, 0
}

// 1,001 keys on the side whose second page fails (the source, the review's case, or the primary),
// with and without a delimiter: the page that needed the failed peek is answered as a failure,
// never as a complete listing short of a key, and once the side answers again the same listing
// pages through every key. Negative control: with the peek's error ignored (as before) the first
// page answers 200, IsTruncated false, 1,000 of 1,001 keys.
func TestMergedListingLookaheadFailureIsNotTheEnd(t *testing.T) {
	for _, side := range []string{"source", "primary"} {
		for _, delim := range []string{"", "&delimiter=%2F"} {
			t.Run(side+delim, func(t *testing.T) {
				m := newMixedRig(t, nil)
				target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
				f, bucket, name := m.garage, "acme-1111-data", "garage"
				if side == "primary" {
					f, bucket, name = m.minio, target, "minio"
				}
				for i := range 1001 {
					f.put(bucket, fmt.Sprintf("key/%04d", i), []byte("v"))
				}
				query := delim
				if delim != "" {
					query += "&prefix=key%2F" // the keys are under key/, so the delimiter leaves them whole
				}
				fail := flakyContinuation(t, m, name, f)
				if keys, status := listAll(t, m, query); status == http.StatusOK {
					t.Fatalf("a listing whose %s's second page failed answered complete with %d of 1001 keys", side, len(keys))
				}
				fail.Store(false)
				keys, status := listAll(t, m, query)
				if status != http.StatusOK || len(keys) != 1001 || keys[0] != "key/0000" || keys[1000] != "key/1000" {
					t.Fatalf("the listing after the side recovered: %d, %d keys", status, len(keys))
				}
			})
		}
	}
}

// Exactly a page of keys: the side's first page says it is the last, so no peek is made, and the
// answer is complete even while continuations would fail.
func TestMergedListingAtAnExactPageBoundary(t *testing.T) {
	m := newMixedRig(t, nil)
	ramp(t, m, directory.Transition{To: directory.StateMigrating})
	for i := range 1000 {
		m.garage.put("acme-1111-data", fmt.Sprintf("key/%04d", i), []byte("v"))
	}
	flakyContinuation(t, m, "garage", m.garage)
	keys, status := listAll(t, m, "")
	if status != http.StatusOK || len(keys) != 1000 {
		t.Fatalf("a listing of exactly one page: %d, %d keys", status, len(keys))
	}
}
