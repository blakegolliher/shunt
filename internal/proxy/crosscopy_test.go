package proxy

// ADR-0014: a copy whose source is on another cluster, or in a bucket whose objects are split
// across two, is streamed by shunt. These tests drive the cases that used to answer 501.

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

func TestCrossClusterCopyStreamsTheObject(t *testing.T) {
	m := newMixedRig(t, nil)
	// acme/old lives on minio, acme/data on garage: a copy between them crosses clusters.
	m.send(t, "PUT", "/old/k", "", []byte("the source bytes"), acmeAK, acmeSK, nil)

	r := m.send(t, "PUT", "/data/x", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/old/k"})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("cross-cluster copy: %d %s", r.StatusCode, r.body)
	}
	if !strings.Contains(string(r.body), "<CopyObjectResult>") || !strings.Contains(string(r.body), "<ETag>") {
		t.Errorf("a copy answers CopyObjectResult with an ETag: %s", r.body)
	}
	if b, _ := m.garage.object("acme-1111-data", "x"); string(b) != "the source bytes" {
		t.Fatalf("the copy did not land on the destination cluster: %q", b)
	}
	if b, _ := m.minio.object("acme-2222-old", "k"); string(b) != "the source bytes" {
		t.Fatalf("the source object changed: %q", b)
	}
	// The destination never saw a copy-source header: no backend could have done this copy.
	if _, hdr := m.garage.last(); hdr.Get("X-Amz-Copy-Source") != "" {
		t.Errorf("the destination was asked to copy for itself: %q", hdr.Get("X-Amz-Copy-Source"))
	}
	m.noLeak(t, "cross-cluster copy", r)

	// A source object that does not exist is NoSuchKey, not a half-written destination.
	r = m.send(t, "PUT", "/data/y", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/old/missing"})
	if r.StatusCode != http.StatusNotFound || !strings.Contains(string(r.body), "NoSuchKey") {
		t.Fatalf("copy of a missing source: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.garage.object("acme-1111-data", "y"); ok {
		t.Error("a failed copy left an object behind")
	}
}

// The copy-source conditions are evaluated where the source object is.
func TestCrossClusterCopyHonoursSourceConditions(t *testing.T) {
	m := newMixedRig(t, nil)
	m.send(t, "PUT", "/old/k", "", []byte("v1"), acmeAK, acmeSK, nil)
	etag := etagOf([]byte("v1"))

	if r := m.send(t, "PUT", "/data/x", "", nil, acmeAK, acmeSK,
		map[string]string{"X-Amz-Copy-Source": "/old/k", "X-Amz-Copy-Source-If-Match": etag}); r.StatusCode != http.StatusOK {
		t.Fatalf("copy with a matching source condition: %d %s", r.StatusCode, r.body)
	}
	r := m.send(t, "PUT", "/data/y", "", nil, acmeAK, acmeSK,
		map[string]string{"X-Amz-Copy-Source": "/old/k", "X-Amz-Copy-Source-If-Match": `"0000"`})
	if r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("copy with a stale source condition: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.garage.object("acme-1111-data", "y"); ok {
		t.Error("a refused copy wrote the destination")
	}
}

// S3A renames by copying and then deleting. During a ramp the two keys can hash to different
// clusters, which is exactly the copy no backend can do.
func TestS3ARenameDuringARamp(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/stay/part-0", []byte("committed bytes"))
	target := ramp(t, m, directory.Transition{To: directory.StateRamping, Prefixes: []string{"moved/"}})

	// The source key stays on the source cluster; the destination key is in the ramp's range.
	r := m.send(t, "PUT", "/data/moved/part-0", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/stay/part-0"})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("rename copy across the ramp: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.minio.object(target, "moved/part-0"); string(b) != "committed bytes" {
		t.Fatalf("the renamed object is not on the target: %q", b)
	}
	if r := m.acme(t, "DELETE", "/data/stay/part-0", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("rename delete: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.garage.object("acme-1111-data", "stay/part-0"); ok {
		t.Error("the renamed-from object is still on the source")
	}
	// The client reads the new name back through shunt, wherever it landed.
	if r := m.acme(t, "GET", "/data/moved/part-0", nil); r.StatusCode != http.StatusOK || string(r.body) != "committed bytes" {
		t.Fatalf("read after rename: %d %q", r.StatusCode, r.body)
	}
}

// A multipart source keeps its part layout, so the copy keeps the ETag shape clients cache.
func TestCrossClusterCopyKeepsThePartLayout(t *testing.T) {
	m := newMixedRig(t, nil)
	parts := [][]byte{bytes.Repeat([]byte("a"), 512), bytes.Repeat([]byte("b"), 512), bytes.Repeat([]byte("c"), 64)}
	m.minio.putMultipart(t, "acme-2222-old", "big", parts)

	r := m.send(t, "PUT", "/data/big", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/old/big"})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("copy of a multipart object: %d %s", r.StatusCode, r.body)
	}
	if !strings.Contains(string(r.body), "-3") {
		t.Errorf("the copy's ETag should still name three parts: %s", r.body)
	}
	got, _ := m.garage.object("acme-1111-data", "big")
	if !bytes.Equal(got, bytes.Join(parts, nil)) {
		t.Fatalf("copied bytes: %d, want %d", len(got), len(bytes.Join(parts, nil)))
	}
	layout := m.garage.partsOf("acme-1111-data", "big")
	if len(layout) != len(parts) {
		t.Fatalf("part layout: %d parts, want %d", len(layout), len(parts))
	}
	for i := range parts {
		if !bytes.Equal(layout[i], parts[i]) {
			t.Fatalf("part %d differs: %d bytes, want %d", i+1, len(layout[i]), len(parts[i]))
		}
	}
}

// UploadPartCopy is how a client copies a large object part by part; its source can be on the
// other cluster too.
func TestCrossClusterUploadPartCopy(t *testing.T) {
	m := newMixedRig(t, nil)
	m.send(t, "PUT", "/old/src", "", []byte("0123456789"), acmeAK, acmeSK, nil)

	r := m.send(t, "POST", "/data/assembled?uploads", "", nil, acmeAK, acmeSK, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("create upload: %d %s", r.StatusCode, r.body)
	}
	id := between(r.body, "<UploadId>", "</UploadId>")
	r = m.send(t, "PUT", "/data/assembled?uploadId="+id+"&partNumber=1", "", nil, acmeAK, acmeSK,
		map[string]string{"X-Amz-Copy-Source": "/old/src", "X-Amz-Copy-Source-Range": "bytes=0-4"})
	if r.StatusCode != http.StatusOK || !strings.Contains(string(r.body), "<CopyPartResult>") {
		t.Fatalf("upload part copy: %d %s", r.StatusCode, r.body)
	}
	r = m.send(t, "POST", "/data/assembled?uploadId="+id, "", []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>`), acmeAK, acmeSK, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("complete: %d %s", r.StatusCode, r.body)
	}
	if b, _ := m.garage.object("acme-1111-data", "assembled"); string(b) != "01234" {
		t.Fatalf("the assembled object: %q", b)
	}
}
