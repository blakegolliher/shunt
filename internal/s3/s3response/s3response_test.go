package s3response

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// fake starts an in-process S3 server (gofakes3, MIT, test-only) with one bucket and n objects.
func fake(t *testing.T, bucket string, n int) *httptest.Server {
	t.Helper()
	be := s3mem.New()
	if err := be.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("dir/%03d.txt", i)
		if _, err := be.PutObject(bucket, key, nil, strings.NewReader("x"), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(gofakes3.New(be).Server())
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec,noctx // test against a local httptest server
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, body)
	}
	if err := xml.Unmarshal(body, v); err != nil {
		t.Fatalf("unmarshal %T: %v\n%s", v, err, body)
	}
}

func TestListBucketsAgainstFake(t *testing.T) {
	srv := fake(t, "alpha", 0)
	var out ListAllMyBucketsResult
	get(t, srv.URL+"/", &out)
	if len(out.Buckets.Bucket) != 1 || out.Buckets.Bucket[0].Name != "alpha" {
		t.Fatalf("buckets: %+v", out.Buckets)
	}
}

func TestListObjectsV2AgainstFake(t *testing.T) {
	srv := fake(t, "b", 5)
	var out ListObjectsV2Result
	get(t, srv.URL+"/b?list-type=2&max-keys=3&prefix=dir/", &out)
	if str(out.Name) != "b" || str(out.Prefix) != "dir/" || i32(out.MaxKeys) != 3 || len(out.Contents) != 3 {
		t.Fatalf("v2 listing: name=%s prefix=%s max=%d n=%d", str(out.Name), str(out.Prefix), i32(out.MaxKeys), len(out.Contents))
	}
	if out.IsTruncated == nil || !*out.IsTruncated || str(out.NextContinuationToken) == "" {
		t.Fatalf("expected truncation with a token: %+v", out)
	}
	first := out.Contents[0]
	if str(first.Key) != "dir/000.txt" || first.Size == nil || *first.Size != 1 || str(first.ETag) == "" {
		t.Fatalf("first object: key=%s size=%v etag=%s", str(first.Key), first.Size, str(first.ETag))
	}
	if first.LastModified == nil || first.LastModified.IsZero() {
		t.Fatal("LastModified not parsed")
	}
}

func TestListObjectsV1AgainstFake(t *testing.T) {
	srv := fake(t, "b", 2)
	var out ListObjectsResult
	get(t, srv.URL+"/b?delimiter=/", &out)
	if len(out.CommonPrefixes) != 1 || out.CommonPrefixes[0].Prefix != "dir/" {
		t.Fatalf("common prefixes: %+v", out.CommonPrefixes)
	}
}

func TestInitiateMultipartAgainstFake(t *testing.T) {
	srv := fake(t, "b", 0)
	resp, err := http.Post(srv.URL+"/b/big.bin?uploads", "application/octet-stream", nil) //nolint:gosec,noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	// gofakes3 omits the S3 XML namespace here (AWS, Garage, and MinIO emit it), so decode into
	// a namespace-free view of the same fields. s3diff in POC-1 checks the real backends.
	var out struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string
		Key      string
		UploadId string //nolint:revive // S3 wire name
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Bucket != "b" || out.Key != "big.bin" || out.UploadId == "" {
		t.Fatalf("initiate: %+v", out)
	}
}

func TestListObjectsV2MarshalShape(t *testing.T) {
	lm := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	in := ListObjectsV2Result{
		Name: ptr("b"), KeyCount: ptr(int32(1)), MaxKeys: ptr(int32(1000)),
		Contents: []Object{{Key: ptr("k"), LastModified: &lm, ETag: ptr(`"e"`), Size: ptr(int64(3)), StorageClass: "STANDARD"}},
	}
	out, err := xml.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// Element presence, not order: the lifted Object.MarshalXML emits LastModified first (RFC 3339).
	for _, want := range []string{
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
		`<Name>b</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><Contents>`,
		`<LastModified>2026-09-14T12:00:00Z</LastModified>`,
		`<Key>k</Key>`, `<ETag>&#34;e&#34;</ETag>`, `<Size>3</Size>`, `<StorageClass>STANDARD</StorageClass>`,
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	var back ListObjectsV2Result
	if err := xml.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Contents) != 1 || str(back.Contents[0].Key) != "k" || !back.Contents[0].LastModified.Equal(lm) {
		t.Fatalf("round trip: %+v", back)
	}
}

func TestTaggingUnmarshal(t *testing.T) {
	const in = `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`
	var tg Tagging
	if err := xml.Unmarshal([]byte(in), &tg); err != nil {
		t.Fatal(err)
	}
	if len(tg.TagSet.Tags) != 1 || tg.TagSet.Tags[0].Key != "a" {
		t.Fatalf("tags: %+v", tg)
	}
}

func TestAmzDate(t *testing.T) {
	for _, in := range []string{"2026-09-14T12:00:00.000Z", "2026-09-14T12:00:00.000000Z", "2026-09-14T12:00:00-0700"} {
		var d struct {
			Date AmzDate
		}
		if err := xml.Unmarshal([]byte("<x><Date>"+in+"</Date></x>"), &d); err != nil {
			t.Errorf("%s: %v", in, err)
		}
	}
	var d struct{ Date AmzDate }
	err := xml.Unmarshal([]byte("<x><Date>yesterday</Date></x>"), &d)
	if !errors.Is(err, ErrInvalidDate) {
		t.Fatalf("want ErrInvalidDate, got %v", err)
	}
}

func FuzzTaggingUnmarshal(f *testing.F) {
	f.Add(`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`)
	f.Add(`<Tagging><TagSet></TagSet></Tagging>`)
	f.Add(`<Tagging>`)
	f.Fuzz(func(_ *testing.T, in string) {
		var tg Tagging
		_ = xml.Unmarshal([]byte(in), &tg) // must not panic
	})
}

func FuzzAmzDate(f *testing.F) {
	f.Add("2026-09-14T12:00:00.000Z")
	f.Add("2026-09-14T12:00:00-0700")
	f.Add("")
	f.Fuzz(func(_ *testing.T, in string) {
		var d struct{ Date AmzDate }
		_ = xml.Unmarshal([]byte("<x><Date>"+in+"</Date></x>"), &d)
	})
}

func BenchmarkMarshalListObjectsV2(b *testing.B) {
	lm := time.Now().UTC()
	in := listing(&lm)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := xml.Marshal(in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalListObjectsV2(b *testing.B) {
	lm := time.Now().UTC()
	in := listing(&lm)
	data, _ := xml.Marshal(in)
	b.ReportAllocs()
	for b.Loop() {
		var out ListObjectsV2Result
		if err := xml.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func i32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func listing(lm *time.Time) ListObjectsV2Result {
	in := ListObjectsV2Result{Name: ptr("b"), KeyCount: ptr(int32(1000)), MaxKeys: ptr(int32(1000))}
	for i := 0; i < 1000; i++ {
		in.Contents = append(in.Contents, Object{Key: ptr(fmt.Sprintf("dir/%06d", i)), LastModified: lm, ETag: ptr(`"e"`), Size: ptr(int64(1)), StorageClass: "STANDARD"})
	}
	return in
}
