package proxy

// ADR-0013 for CompleteMultipartUpload (third review, R3-06): a completion is routed as an upload,
// pinned by its uploadId to the cluster that holds its parts, and still commits the logical object
// its conditions are about, so they are judged against both clusters of a moving bucket.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

// completeWith begins an upload of key through shunt, sends one part, and completes it with hdr.
func completeWith(t *testing.T, m *mixedRig, key string, hdr map[string]string) reply {
	t.Helper()
	init := m.acme(t, "POST", "/data/"+key+"?uploads", nil)
	id := between(init.body, "<UploadId>", "</UploadId>")
	if init.StatusCode != http.StatusOK || id == "" {
		t.Fatalf("init: %d %s", init.StatusCode, init.body)
	}
	return completeUpload(t, m, key, id, hdr)
}

func completeUpload(t *testing.T, m *mixedRig, key, id string, hdr map[string]string) reply {
	t.Helper()
	part := m.acme(t, "PUT", "/data/"+key+"?partNumber=1&uploadId="+id, []byte("the upload's bytes"))
	if part.StatusCode != http.StatusOK {
		t.Fatalf("part: %d %s", part.StatusCode, part.body)
	}
	body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + part.Header.Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`)
	return m.send(t, "POST", "/data/"+key+"?uploadId="+id, "", body, acmeAK, acmeSK, hdr)
}

// Each condition a completion can carry, against where the object's current version is, while the
// bucket is MIGRATING (new uploads land on the target, minio): create-once over a source-only
// object is refused and writes nothing (the review's case); create-once over nothing creates it;
// update-if-current matching the source's version completes, converted to create-only on the target;
// update-if-current naming another version is refused. Negative control: without R3-06 the
// completion is never checked against the source and the first and last cases answer 200.
func TestConditionalCompletionIsJudgedOnBothClusters(t *testing.T) {
	cases := []struct {
		name     string
		onSource bool
		hdr      func(sourceETag string) map[string]string
		want     int
	}{
		{"create-once over a source-only object", true, func(string) map[string]string { return map[string]string{"If-None-Match": "*"} }, http.StatusPreconditionFailed},
		{"create-once over nothing", false, func(string) map[string]string { return map[string]string{"If-None-Match": "*"} }, http.StatusOK},
		{"update-if-current, the source's version", true, func(e string) map[string]string { return map[string]string{"If-Match": `"` + e + `"`} }, http.StatusOK},
		{"update-if-current, another version", true, func(string) map[string]string { return map[string]string{"If-Match": `"0123456789abcdef"`} }, http.StatusPreconditionFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newMixedRig(t, nil)
			target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
			etag := ""
			if c.onSource {
				etag = putOnSource(t, m, "k", []byte("first acknowledged value"))
			}
			r := completeWith(t, m, "k", c.hdr(etag))
			if r.StatusCode != c.want {
				t.Fatalf("completion: %d %s, want %d", r.StatusCode, r.body, c.want)
			}
			got, ok := m.minio.object(target, "k")
			if c.want == http.StatusPreconditionFailed && ok {
				t.Fatalf("a refused completion wrote the object on the target: %q", got)
			}
			if c.want == http.StatusOK && string(got) != "the upload's bytes" {
				t.Fatalf("a completed upload is not on the target: %q %v", got, ok)
			}
			if c.onSource {
				if b, _ := m.garage.object("acme-1111-data", "k"); string(b) != "first acknowledged value" {
					t.Fatalf("the source's version changed: %q", b)
				}
			}
		})
	}
}

// An upload begun before the move is pinned to the source and completes there; its create-once
// is judged against the target too, which may hold the key by then.
func TestConditionalCompletionPinnedToTheSource(t *testing.T) {
	m := newMixedRig(t, nil)
	init := m.acme(t, "POST", "/data/k?uploads", nil)
	id := between(init.body, "<UploadId>", "</UploadId>")
	if id == "" {
		t.Fatalf("init: %d %s", init.StatusCode, init.body)
	}
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	m.minio.put(target, "k", []byte("written on the target meanwhile"))
	r := completeUpload(t, m, "k", id, map[string]string{"If-None-Match": "*"})
	if r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("create-once completion pinned to the source, the key on the target: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.garage.object("acme-1111-data", "k"); ok {
		t.Fatal("the refused completion wrote the object on the source")
	}
}

// A completion whose condition cannot be checked on the other cluster (its HEAD answers 500) is
// refused with 503 and not sent: guessing either way breaks the promise.
func TestConditionalCompletionUncheckable(t *testing.T) {
	m := newMixedRig(t, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		m.garage.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	cl := m.clusters["garage"]
	cl.Endpoints = []string{strings.TrimPrefix(srv.URL, "http://")}
	if err := m.dir.PutCluster(context.Background(), "garage", cl, "", "test"); err != nil {
		t.Fatal(err)
	}
	target := ramp(t, m, directory.Transition{To: directory.StateMigrating})
	r := completeWith(t, m, "k", map[string]string{"If-None-Match": "*"})
	if r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a completion whose condition could not be checked: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.minio.object(target, "k"); ok {
		t.Fatal("an unchecked completion was sent")
	}
}
