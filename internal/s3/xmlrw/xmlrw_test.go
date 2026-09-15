package xmlrw

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

const backend = "e2e-a-3f9c-data"

// identity returns every text unchanged.
type identity struct{}

func (identity) Edit(dst []byte, _ Field, text []byte) []byte { return append(dst, text...) }

// renamer is a small stand-in for the proxy's editor.
type renamer struct{}

func (renamer) Edit(dst []byte, f Field, text []byte) []byte {
	switch f {
	case FieldBucket:
		if string(text) == backend {
			return append(dst, "data"...)
		}
	case FieldUploadID:
		if len(text) > 0 {
			dst = append(dst, "0f3a9c~"...)
		}
	case FieldLocation:
		return append(dst, "https://s3.example/data/dir/multipart.bin"...)
	case FieldResource, FieldMessage:
		return append(dst, bytes.ReplaceAll(text, []byte(backend), []byte("data"))...)
	case FieldEndpoint:
		return append(dst, "s3.example"...)
	}
	return append(dst, text...)
}

type fixture struct {
	name, in, want string // want is the renamer's output; "" means identical to in
	overflows      int
}

var fixtures = []fixture{
	{
		name: "minio ListObjectsV2",
		in:   `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>e2e-a-3f9c-data</Name><Prefix>dir/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>dir/a&amp;b.bin</Key><LastModified>2026-09-15T18:00:00.000Z</LastModified><ETag>&quot;abc&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>e2e-a-3f9c-data</Key><Size>2</Size></Contents></ListBucketResult>`,
		want: `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>data</Name><Prefix>dir/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>dir/a&amp;b.bin</Key><LastModified>2026-09-15T18:00:00.000Z</LastModified><ETag>&quot;abc&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>e2e-a-3f9c-data</Key><Size>2</Size></Contents></ListBucketResult>`,
	},
	{
		name: "ListObjectVersions",
		in:   `<ListVersionsResult><Name>e2e-a-3f9c-data</Name><Version><Key>k</Key></Version></ListVersionsResult>`,
		want: `<ListVersionsResult><Name>data</Name><Version><Key>k</Key></Version></ListVersionsResult>`,
	},
	{
		name: "ListMultipartUploads",
		in:   `<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>e2e-a-3f9c-data</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><NextKeyMarker>k2</NextKeyMarker><NextUploadIdMarker>up2</NextUploadIdMarker><MaxUploads>2</MaxUploads><IsTruncated>true</IsTruncated><Upload><Key>k1</Key><UploadId>up1~x</UploadId><Initiator><ID>i</ID></Initiator></Upload><Upload><Key>k2</Key><UploadId>up2</UploadId></Upload></ListMultipartUploadsResult>`,
		want: `<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>data</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><NextKeyMarker>k2</NextKeyMarker><NextUploadIdMarker>0f3a9c~up2</NextUploadIdMarker><MaxUploads>2</MaxUploads><IsTruncated>true</IsTruncated><Upload><Key>k1</Key><UploadId>0f3a9c~up1~x</UploadId><Initiator><ID>i</ID></Initiator></Upload><Upload><Key>k2</Key><UploadId>0f3a9c~up2</UploadId></Upload></ListMultipartUploadsResult>`,
	},
	{
		name: "InitiateMultipartUpload",
		in:   `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>e2e-a-3f9c-data</Bucket><Key>dir/multipart.bin</Key><UploadId>VXBsb2FkIElE</UploadId></InitiateMultipartUploadResult>`,
		want: `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>data</Bucket><Key>dir/multipart.bin</Key><UploadId>0f3a9c~VXBsb2FkIElE</UploadId></InitiateMultipartUploadResult>`,
	},
	{
		name: "ListParts",
		in:   `<ListPartsResult><Bucket>e2e-a-3f9c-data</Bucket><Key>k</Key><UploadId>abc</UploadId><Part><PartNumber>1</PartNumber></Part></ListPartsResult>`,
		want: `<ListPartsResult><Bucket>data</Bucket><Key>k</Key><UploadId>0f3a9c~abc</UploadId><Part><PartNumber>1</PartNumber></Part></ListPartsResult>`,
	},
	{
		name: "minio CompleteMultipartUpload with keepalive whitespace",
		in:   "\n \n" + `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://127.0.0.1:9000/e2e-a-3f9c-data/dir/multipart.bin</Location><Bucket>e2e-a-3f9c-data</Bucket><Key>dir/multipart.bin</Key><ETag>&#34;abc-2&#34;</ETag></CompleteMultipartUploadResult>`,
		want: "\n \n" + `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>https://s3.example/data/dir/multipart.bin</Location><Bucket>data</Bucket><Key>dir/multipart.bin</Key><ETag>&#34;abc-2&#34;</ETag></CompleteMultipartUploadResult>`,
	},
	{
		name: "minio error",
		in:   `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>does/not/exist</Key><BucketName>e2e-a-3f9c-data</BucketName><Resource>/e2e-a-3f9c-data/does/not/exist</Resource><RequestId>1866</RequestId><HostId>dd9025</HostId></Error>`,
		want: `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>does/not/exist</Key><BucketName>data</BucketName><Resource>/data/does/not/exist</Resource><RequestId>1866</RequestId><HostId>dd9025</HostId></Error>`,
	},
	{
		name: "garage error with region",
		in:   `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>Bucket not found: e2e-a-3f9c-data</Message><Resource>/e2e-a-3f9c-data/k</Resource><Region>garage</Region></Error>`,
		want: `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>Bucket not found: data</Message><Resource>/data/k</Resource><Region>garage</Region></Error>`,
	},
	{
		name: "aws redirect error",
		in:   `<Error><Code>PermanentRedirect</Code><Message>m</Message><Endpoint>e2e-a-3f9c-data.s3.eu-west-1.amazonaws.com</Endpoint><Bucket>e2e-a-3f9c-data</Bucket></Error>`,
		want: `<Error><Code>PermanentRedirect</Code><Message>m</Message><Endpoint>s3.example</Endpoint><Bucket>data</Bucket></Error>`,
	},
	{
		name: "comments, PI, doctype, prefixes, quoted '>' in attributes, deeper Name",
		in:   `<?xml version="1.0"?><!-- a > comment -- with dashes --><?pi a?b ?><!DOCTYPE x><s3:ListBucketResult xmlns:s3="ns" a='x>y' b="p/q"><s3:Name >e2e-a-3f9c-data</s3:Name ><Other><Name>e2e-a-3f9c-data</Name></Other></s3:ListBucketResult>`,
		want: `<?xml version="1.0"?><!-- a > comment -- with dashes --><?pi a?b ?><!DOCTYPE x><s3:ListBucketResult xmlns:s3="ns" a='x>y' b="p/q"><s3:Name >data</s3:Name ><Other><Name>e2e-a-3f9c-data</Name></Other></s3:ListBucketResult>`,
	},
	{name: "CDATA inside a target is forwarded verbatim", in: `<ListBucketResult><Name><![CDATA[e2e-a-3f9c-data]]></Name></ListBucketResult>`, overflows: 1},
	{name: "child element inside a target is forwarded verbatim", in: `<Error><Message>a<b>e2e-a-3f9c-data</b></Message></Error>`, overflows: 1},
	{name: "comment inside a target is forwarded verbatim", in: `<Error><Resource>/e2e-a-3f9c-data<!-- x --></Resource></Error>`, overflows: 1},
	{
		name: "self-closing and empty targets",
		in:   `<InitiateMultipartUploadResult><Bucket/><UploadId></UploadId><Key>k</Key></InitiateMultipartUploadResult>`,
	},
	{name: "not XML", in: "hello <Name>e2e-a-3f9c-data</Name>"},
	{name: "unknown root", in: `<CopyObjectResult><ETag>e2e-a-3f9c-data</ETag><Name>e2e-a-3f9c-data</Name></CopyObjectResult>`},
	{
		name: "BOM",
		in:   "\xEF\xBB\xBF<ListBucketResult><Name>e2e-a-3f9c-data</Name></ListBucketResult>",
		want: "\xEF\xBB\xBF<ListBucketResult><Name>data</Name></ListBucketResult>",
	},
	{
		name: "a once-rule matches only the first occurrence; the rest passes through",
		in:   `<ListBucketResult><Name>e2e-a-3f9c-data</Name><Name>e2e-a-3f9c-data</Name></ListBucketResult>`,
		want: `<ListBucketResult><Name>data</Name><Name>e2e-a-3f9c-data</Name></ListBucketResult>`,
	},
	{name: "truncated inside a target", in: `<ListBucketResult><Name>e2e-a-3f9`},
	{name: "empty", in: ""},
}

func run(t testing.TB, in []byte, ed Editor, splits []int, maxText int) (string, *Writer) {
	t.Helper()
	var out bytes.Buffer
	w := New(&out, ed, maxText)
	prev := 0
	for _, s := range splits {
		if s < prev || s > len(in) {
			continue
		}
		if _, err := w.Write(in[prev:s]); err != nil {
			t.Fatal(err)
		}
		prev = s
	}
	if _, err := w.Write(in[prev:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return out.String(), w
}

// TestIdentityAtEverySplit is the primary invariant: with an editor that changes nothing, output
// equals input byte for byte whatever the split.
func TestIdentityAtEverySplit(t *testing.T) {
	for _, fx := range fixtures {
		in := []byte(fx.in)
		for k := 0; k <= len(in); k++ {
			if got, _ := run(t, in, identity{}, []int{k}, 0); got != fx.in {
				t.Fatalf("%s: split at %d changed the bytes:\n got %q\nwant %q", fx.name, k, got, fx.in)
			}
		}
	}
}

func TestRewriteAtEverySplit(t *testing.T) {
	for _, fx := range fixtures {
		want := fx.want
		if want == "" {
			want = fx.in
		}
		in := []byte(fx.in)
		whole, w := run(t, in, renamer{}, nil, 0)
		if whole != want {
			t.Errorf("%s:\n got %s\nwant %s", fx.name, whole, want)
			continue
		}
		if w.Overflows != fx.overflows {
			t.Errorf("%s: overflows %d, want %d", fx.name, w.Overflows, fx.overflows)
		}
		for k := 0; k <= len(in); k++ {
			for _, j := range []int{k, k + 1, k + 7} {
				if got, _ := run(t, in, renamer{}, []int{k, j}, 0); got != want {
					t.Fatalf("%s: splits %d,%d:\n got %s\nwant %s", fx.name, k, j, got, want)
				}
			}
		}
	}
}

func TestByteAtATime(t *testing.T) {
	for _, fx := range fixtures {
		want := fx.want
		if want == "" {
			want = fx.in
		}
		splits := make([]int, len(fx.in))
		for i := range splits {
			splits[i] = i
		}
		if got, _ := run(t, []byte(fx.in), renamer{}, splits, 0); got != want {
			t.Errorf("%s one byte at a time:\n got %s\nwant %s", fx.name, got, want)
		}
	}
}

func TestOverflowForwardsVerbatim(t *testing.T) {
	long := strings.Repeat("x", 100)
	in := `<Error><Message>` + long + backend + `</Message><Resource>/` + backend + `</Resource></Error>`
	for _, splits := range [][]int{nil, {20}, {30, 60, 90}} {
		got, w := run(t, []byte(in), renamer{}, splits, 50)
		want := `<Error><Message>` + long + backend + `</Message><Resource>/data</Resource></Error>`
		if got != want || w.Overflows != 1 || w.Rewrites != 1 {
			t.Fatalf("splits %v: overflows %d rewrites %d\n got %s\nwant %s", splits, w.Overflows, w.Rewrites, got, want)
		}
	}
}

func TestStopsScanningAfterLastOnceRule(t *testing.T) {
	var out bytes.Buffer
	w := New(&out, renamer{}, 0)
	_, _ = w.Write([]byte(`<ListBucketResult><Name>e2e-a-3f9c-data</Name><Contents>`))
	if !w.done {
		t.Fatal("scanner still active after the only rule matched")
	}
}

func TestResetReuses(t *testing.T) {
	var a, b bytes.Buffer
	w := New(&a, renamer{}, 0)
	_, _ = w.Write([]byte(fixtures[0].in))
	_ = w.Flush()
	w.Reset(&b, renamer{})
	_, _ = w.Write([]byte(fixtures[3].in))
	_ = w.Flush()
	if a.String() != fixtures[0].want || b.String() != fixtures[3].want {
		t.Fatalf("reset: %s | %s", a.String(), b.String())
	}
}

type failWriter struct{ after int }

func (f *failWriter) Write(p []byte) (int, error) {
	if f.after <= 0 {
		return 0, io.ErrClosedPipe
	}
	f.after--
	return len(p), nil
}

func TestDestinationErrorPropagates(t *testing.T) {
	w := New(&failWriter{after: 0}, renamer{}, 0)
	if _, err := w.Write([]byte(fixtures[3].in)); err == nil {
		t.Fatal("write error swallowed")
	}
}

func FuzzRewrite(f *testing.F) {
	for _, fx := range fixtures {
		f.Add([]byte(fx.in), uint16(len(fx.in)/2), uint16(len(fx.in)/3))
	}
	f.Fuzz(func(t *testing.T, in []byte, s1, s2 uint16) {
		a, b := int(s1)%(len(in)+1), int(s2)%(len(in)+1)
		if a > b {
			a, b = b, a
		}
		if got, _ := run(t, in, identity{}, []int{a, b}, 16); got != string(in) {
			t.Fatalf("identity broken at splits %d,%d:\n got %q\nwant %q", a, b, got, in)
		}
		whole, _ := run(t, in, renamer{}, nil, 16)
		if split, _ := run(t, in, renamer{}, []int{a, b}, 16); split != whole {
			t.Fatalf("output depends on the split %d,%d:\nwhole %q\nsplit %q", a, b, whole, split)
		}
	})
}

func listing(n int) []byte {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>e2e-a-3f9c-data</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `<Contents><Key>dir/object-%06d.bin</Key><LastModified>2026-09-15T18:00:00.000Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`, i, i*17)
	}
	b.WriteString(`</ListBucketResult>`)
	return b.Bytes()
}

func uploads(n int) []byte {
	var b bytes.Buffer
	b.WriteString(`<ListMultipartUploadsResult><Bucket>e2e-a-3f9c-data</Bucket><UploadIdMarker></UploadIdMarker>`)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `<Upload><Key>dir/object-%06d.bin</Key><UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRz%06d</UploadId><Initiated>2026-09-15T18:00:00.000Z</Initiated></Upload>`, i, i)
	}
	b.WriteString(`</ListMultipartUploadsResult>`)
	return b.Bytes()
}

func benchDoc(b *testing.B, doc []byte) {
	w := New(io.Discard, renamer{}, 0)
	chunk := 256 << 10
	b.SetBytes(int64(len(doc)))
	b.ReportAllocs()
	for b.Loop() {
		w.Reset(io.Discard, renamer{})
		for off := 0; off < len(doc); off += chunk {
			_, _ = w.Write(doc[off:min(off+chunk, len(doc))])
		}
		_ = w.Flush()
	}
}

// A 1000-key listing: the Name rule matches first, then the scanner stops.
func BenchmarkListing1000(b *testing.B) { benchDoc(b, listing(1000)) }

// 1000 uploads: every UploadId is rewritten, so the whole document is scanned.
func BenchmarkUploads1000(b *testing.B) { benchDoc(b, uploads(1000)) }
