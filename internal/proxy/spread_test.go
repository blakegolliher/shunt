package proxy

// ADR-0018 N2: a client bucket spread over two legs, garage and minio, driven through the handler.
// Every key's requests go to the leg that owns its hash, and listings merge both legs.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// spread makes acme/spread a bucket spread over garage (spread-g) and minio (spread-m).
func spread(t testing.TB, m *mixedRig) *directory.Placement {
	t.Helper()
	m.garage.addBucket("spread-g")
	m.minio.addBucket("spread-m")
	m.backendNames = append(m.backendNames, "spread-g", "spread-m")
	legs := []directory.Leg{{Cluster: "garage", Bucket: "spread-g"}, {Cluster: "minio", Bucket: "spread-m"}}
	if err := m.dir.CreateSpread(context.Background(), "acme", "spread", legs, "test"); err != nil {
		t.Fatal(err)
	}
	p, _ := m.dir.Snapshot().Lookup("acme", "spread")
	return p
}

// legBucket names the fake and bucket that own key, and the other leg's.
func legBucket(t testing.TB, m *mixedRig, p *directory.Placement, key string) (owner *fakeS3, ownBucket string, other *fakeS3, otherBucket string) {
	t.Helper()
	id, err := migrate.OwnerOf(p, key)
	if err != nil {
		t.Fatal(err)
	}
	if id == "garage" {
		return m.garage, "spread-g", m.minio, "spread-m"
	}
	return m.minio, "spread-m", m.garage, "spread-g"
}

// put writes an object straight to a fake cluster, behind shunt's back.
func (f *fakeS3) put(bucket, key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buckets[bucket][key] = data
	f.wrote(bucket, key)
}

func listKeys(t testing.TB, body []byte, tag string) []string {
	t.Helper()
	var out []string
	for rest := string(body); ; {
		i := strings.Index(rest, "<"+tag+">")
		if i < 0 {
			return out
		}
		rest = rest[i+len(tag)+2:]
		out = append(out, rest[:strings.Index(rest, "</"+tag+">")])
	}
}

// Writes, reads, heads and deletes of a key reach only the leg that owns it, and both legs own
// some of 60 keys.
func TestSpreadRoutesEachKeyToItsOwner(t *testing.T) {
	m := newMixedRig(t, nil)
	p := spread(t, m)
	perLeg := map[*fakeS3]int{}
	for i := range 60 {
		key := fmt.Sprintf("obj/%03d", i)
		if r := m.acme(t, "PUT", "/spread/"+key, []byte("v-"+key)); r.StatusCode != 200 {
			t.Fatalf("PUT %s: %d %s", key, r.StatusCode, r.body)
		}
		owner, ob, other, xb := legBucket(t, m, p, key)
		if b, ok := owner.object(ob, key); !ok || string(b) != "v-"+key {
			t.Fatalf("%s is not on its owner %s: %q %v", key, ob, b, ok)
		}
		if _, ok := other.object(xb, key); ok {
			t.Fatalf("%s also landed on %s", key, xb)
		}
		perLeg[owner]++
		if r := m.acme(t, "GET", "/spread/"+key, nil); r.StatusCode != 200 || string(r.body) != "v-"+key {
			t.Fatalf("GET %s: %d %q", key, r.StatusCode, r.body)
		}
		if r := m.acme(t, "HEAD", "/spread/"+key, nil); r.StatusCode != 200 {
			t.Fatalf("HEAD %s: %d", key, r.StatusCode)
		}
	}
	if perLeg[m.garage] == 0 || perLeg[m.minio] == 0 {
		t.Fatalf("60 keys did not use both legs: garage %d, minio %d", perLeg[m.garage], perLeg[m.minio])
	}
	if r := m.acme(t, "DELETE", "/spread/obj/007", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %d %s", r.StatusCode, r.body)
	}
	if owner, ob, _, _ := legBucket(t, m, p, "obj/007"); func() bool { _, ok := owner.object(ob, "obj/007"); return ok }() {
		t.Fatal("delete did not reach the owner")
	}
	if r := m.acme(t, "GET", "/spread/obj/007", nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete: %d", r.StatusCode)
	}
	if r := m.acme(t, "HEAD", "/spread", nil); r.StatusCode != 200 {
		t.Fatalf("HeadBucket: %d", r.StatusCode)
	}
	if r := m.acme(t, "GET", "/spread?versioning", nil); r.StatusCode != http.StatusNotImplemented || !strings.Contains(string(r.body), "spread over 2 backend buckets") {
		t.Fatalf("a bucket-level request every leg would need: %d %s", r.StatusCode, r.body)
	}
	m.noLeak(t, "spread GET", m.acme(t, "GET", "/spread/obj/001", nil))
}

// A multipart upload runs on the leg that owns its key, from create through complete.
func TestSpreadMultipartUploadStaysOnItsOwner(t *testing.T) {
	m := newMixedRig(t, nil)
	p := spread(t, m)
	for _, key := range []string{"mpu/a", "mpu/b", "mpu/c", "mpu/d"} {
		r := m.acme(t, "POST", "/spread/"+key+"?uploads", nil)
		if r.StatusCode != 200 {
			t.Fatalf("create %s: %d %s", key, r.StatusCode, r.body)
		}
		id := between(r.body, "<UploadId>", "</UploadId>")
		part := m.acme(t, "PUT", "/spread/"+key+"?partNumber=1&uploadId="+id, []byte("part-one"))
		if part.StatusCode != 200 {
			t.Fatalf("part %s: %d %s", key, part.StatusCode, part.body)
		}
		done := m.acme(t, "POST", "/spread/"+key+"?uploadId="+id,
			[]byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>`+part.Header.Get("ETag")+`</ETag></Part></CompleteMultipartUpload>`))
		if done.StatusCode != 200 {
			t.Fatalf("complete %s: %d %s", key, done.StatusCode, done.body)
		}
		owner, ob, other, xb := legBucket(t, m, p, key)
		if _, ok := owner.object(ob, key); !ok {
			t.Fatalf("%s is not on its owner", key)
		}
		if _, ok := other.object(xb, key); ok {
			t.Fatalf("%s landed on the other leg", key)
		}
	}
}

// The listing merges both legs in order, pages with a token that does not grow with the legs,
// keeps a common prefix both legs hold once, and hides an object a leg holds but does not own.
func TestSpreadListingMergesPagesAndFilters(t *testing.T) {
	m := newMixedRig(t, nil)
	p := spread(t, m)
	want := make([]string, 0, 46)
	for i := range 40 {
		key := fmt.Sprintf("k/%02d", i)
		m.acme(t, "PUT", "/spread/"+key, []byte("x"))
		want = append(want, key)
	}
	for _, key := range []string{"a/1", "a/2", "a/3", "a/4", "b", "z"} {
		m.acme(t, "PUT", "/spread/"+key, []byte("x"))
		want = append(want, key)
	}
	slices.Sort(want)
	// A stray: a key on the leg that does not own it, as a finished move could leave behind.
	_, _, other, xb := legBucket(t, m, p, "stray")
	other.put(xb, "stray", []byte("not mine"))

	var got []string
	token := ""
	for page := 0; page < 30; page++ {
		target := "/spread?list-type=2&max-keys=7"
		if token != "" {
			target += "&continuation-token=" + token
		}
		r := m.acme(t, "GET", target, nil)
		if r.StatusCode != 200 {
			t.Fatalf("page %d: %d %s", page, r.StatusCode, r.body)
		}
		got = append(got, listKeys(t, r.body, "Key")...)
		if !strings.Contains(string(r.body), "<IsTruncated>true</IsTruncated>") {
			break
		}
		token = between(r.body, "<NextContinuationToken>", "</NextContinuationToken>")
		if len(token) > 64 {
			t.Fatalf("the token grows: %d bytes", len(token))
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged listing:\n got  %v\n want %v", got, want)
	}

	// Delimited, one entry per page, so a page ends on a common prefix both legs hold: "a/" once.
	var names []string
	token = ""
	for page := 0; page < 20; page++ {
		target := "/spread?list-type=2&delimiter=/&max-keys=1"
		if token != "" {
			target += "&continuation-token=" + token
		}
		r := m.acme(t, "GET", target, nil)
		names = append(names, listKeys(t, r.body, "Key")...)
		names = append(names, listKeys(t, r.body, "Prefix")[1:]...) // [0] is the request's <Prefix>
		if !strings.Contains(string(r.body), "<IsTruncated>true</IsTruncated>") {
			break
		}
		token = between(r.body, "<NextContinuationToken>", "</NextContinuationToken>")
	}
	if strings.Join(names, ",") != "a/,b,k/,z" {
		t.Fatalf("delimited pages: %v", names)
	}

	// ListObjects v1 pages by marker to the same keys.
	got = nil
	marker := ""
	for page := 0; page < 30; page++ {
		target := "/spread?max-keys=9"
		if marker != "" {
			target += "&marker=" + marker
		}
		r := m.acme(t, "GET", target, nil)
		if r.StatusCode != 200 {
			t.Fatalf("v1 page %d: %d %s", page, r.StatusCode, r.body)
		}
		keys := listKeys(t, r.body, "Key")
		got = append(got, keys...)
		if !strings.Contains(string(r.body), "<IsTruncated>true</IsTruncated>") {
			break
		}
		marker = between(r.body, "<NextMarker>", "</NextMarker>")
		if marker != keys[len(keys)-1] {
			t.Fatalf("NextMarker %q is not the last key %q", marker, keys[len(keys)-1])
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("v1 listing:\n got  %v\n want %v", got, want)
	}

	if r := m.acme(t, "GET", "/spread?list-type=2&continuation-token=garbage!", nil); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("a token shunt did not issue: %d %s", r.StatusCode, r.body)
	}
}

// A leg whose bucket is gone fails the listing: an answer from the other leg alone would be
// silently incomplete.
func TestSpreadListingFailsWithoutALeg(t *testing.T) {
	m := newMixedRig(t, nil)
	spread(t, m)
	m.acme(t, "PUT", "/spread/x", []byte("x"))
	m.minio.dropBucket("spread-m")
	if r := m.acme(t, "GET", "/spread?list-type=2", nil); r.StatusCode < 500 {
		t.Fatalf("listing with a leg missing: %d %s", r.StatusCode, r.body)
	}
}

// A copy between a spread bucket and a plain one reads the source from its owner and writes the
// destination to its owner.
func TestSpreadCopyBothWays(t *testing.T) {
	m := newMixedRig(t, nil)
	p := spread(t, m)
	m.acme(t, "PUT", "/spread/src", []byte("from the spread bucket"))
	if r := m.send(t, "PUT", "/data/copied", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/spread/src"}); r.StatusCode != 200 {
		t.Fatalf("copy out of the spread bucket: %d %s", r.StatusCode, r.body)
	}
	if b, ok := m.garage.object("acme-1111-data", "copied"); !ok || string(b) != "from the spread bucket" {
		t.Fatalf("copied object: %q %v", b, ok)
	}
	if r := m.send(t, "PUT", "/spread/back", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/data/copied"}); r.StatusCode != 200 {
		t.Fatalf("copy into the spread bucket: %d %s", r.StatusCode, r.body)
	}
	owner, ob, _, _ := legBucket(t, m, p, "back")
	if b, ok := owner.object(ob, "back"); !ok || string(b) != "from the spread bucket" {
		t.Fatalf("copy did not land on the owner of its key: %q %v", b, ok)
	}
}

// BenchmarkSpreadListingPage lists a 1000-key page of a bucket spread over n legs, its keys written
// through shunt so each leg holds only the keys it owns, as a spread bucket does. legs=1 is a plain
// bucket, the baseline (ADR-0018, ADR-0019).
func BenchmarkSpreadListingPage(b *testing.B) {
	for _, n := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("legs=%d", n), func(b *testing.B) {
			addrs := make([]string, 0, max(n-2, 0))
			names := make([]string, 0, max(n-2, 0))
			for i := 2; i < n; i++ {
				bucket := fmt.Sprintf("spread-%02d", i)
				f := newFakeS3(b, fmt.Sprintf("c%02d", i), "GARAGECLUSTERKEY", "garage-cluster-secret", bucket)
				srv := httptest.NewServer(f)
				b.Cleanup(srv.Close)
				addrs, names = append(addrs, strings.TrimPrefix(srv.URL, "http://")), append(names, bucket)
			}
			m := newMixedRig(b, nil, func(cs map[string]config.Cluster) {
				for i, a := range addrs {
					cs[fmt.Sprintf("c%02d", i+2)] = config.Cluster{Type: "s3", Scheme: "http", Region: "garage", EndpointMode: "static",
						Endpoints: []string{a}, Credentials: config.Credentials{AccessKey: "GARAGECLUSTERKEY", SecretRef: "env:G"}}
				}
			})
			bucket := "data"
			if n > 1 {
				m.garage.addBucket("spread-g")
				m.minio.addBucket("spread-m")
				legs := []directory.Leg{{Cluster: "garage", Bucket: "spread-g"}, {Cluster: "minio", Bucket: "spread-m"}}
				for i, name := range names {
					legs = append(legs, directory.Leg{Cluster: fmt.Sprintf("c%02d", i+2), Bucket: name})
				}
				if err := m.dir.CreateSpread(context.Background(), "acme", "spread", legs, "test"); err != nil {
					b.Fatal(err)
				}
				bucket = "spread"
			}
			for i := range 1000 {
				if r := m.acme(b, "PUT", fmt.Sprintf("/%s/obj/%06d", bucket, i), []byte("x")); r.StatusCode != 200 {
					b.Fatalf("seed: %d %s", r.StatusCode, r.body)
				}
			}
			b.ResetTimer()
			for b.Loop() {
				if r := m.acme(b, "GET", "/"+bucket+"?list-type=2", nil); r.StatusCode != 200 || strings.Count(string(r.body), "<Key>") != 1000 {
					b.Fatalf("%d, %d keys", r.StatusCode, strings.Count(string(r.body), "<Key>"))
				}
			}
		})
	}
}

// FuzzSpreadToken holds the continuation token parser to a round trip: whatever it accepts, it
// writes back to the same token.
func FuzzSpreadToken(f *testing.F) {
	f.Add(spreadToken{Last: "a/"}.encode())
	f.Add(spreadToken{Last: "k/07", Prefix: true, Done: []string{"garage"}}.encode())
	f.Add("")
	f.Add("e30")
	f.Fuzz(func(t *testing.T, s string) {
		tok, err := decodeSpreadToken(s)
		if err != nil {
			return
		}
		back, err := decodeSpreadToken(tok.encode())
		if err != nil || back.Last != tok.Last || back.Prefix != tok.Prefix || !slices.Equal(back.Done, tok.Done) {
			t.Fatalf("round trip of %+v: %+v %v", tok, back, err)
		}
	})
}
