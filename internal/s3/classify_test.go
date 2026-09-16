package s3

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestEveryRow builds the minimal request each table row describes and asserts the row wins.
// Rows deliberately shadowed by an earlier, more specific row are still asserted: the request
// built from the row's own discriminators must not match an earlier row.
func TestEveryRow(t *testing.T) {
	for i, r := range rules {
		q := url.Values{}
		for _, k := range r.query {
			q[k] = []string{"x"}
		}
		if r.value[0] != "" {
			q.Set(r.value[0], r.value[1])
		}
		h := http.Header{}
		if r.header != "" {
			h.Set(r.header, "src/key")
		}
		name := fmt.Sprintf("%02d_%s_%s_%s", i, r.method, r.level, r.op)
		t.Run(name, func(t *testing.T) {
			if got := classify(r.method, r.level, q, h); got != r.op {
				t.Fatalf("row %d (%s %s %v %v %s): got %s want %s", i, r.method, r.level, r.query, r.value, r.header, got, r.op)
			}
		})
	}
}

func TestPrecedence(t *testing.T) {
	cases := []struct {
		name   string
		method string
		level  Level
		query  string
		header map[string]string
		want   Op
	}{
		{"uploadId before uploads on object POST", "POST", LevelObject, "uploads&uploadId=1", nil, OpCreateMultipartUpload},
		{"uploadId+partNumber+copy-source is UploadPartCopy", "PUT", LevelObject, "uploadId=1&partNumber=2", map[string]string{"X-Amz-Copy-Source": "b/k"}, OpUploadPartCopy},
		{"uploadId+partNumber without copy-source is UploadPart", "PUT", LevelObject, "uploadId=1&partNumber=2", nil, OpUploadPart},
		{"copy-source alone is CopyObject", "PUT", LevelObject, "", map[string]string{"X-Amz-Copy-Source": "b/k"}, OpCopyObject},
		{"uploadId without partNumber is not UploadPart", "PUT", LevelObject, "uploadId=1", nil, OpPutObject},
		{"partNumber on GET is still GetObject", "GET", LevelObject, "partNumber=3", nil, OpGetObject},
		{"versionId on GET is still GetObject", "GET", LevelObject, "versionId=abc", nil, OpGetObject},
		{"versionId on DELETE is still DeleteObject", "DELETE", LevelObject, "versionId=abc", nil, OpDeleteObject},
		{"list-type=2 exact", "GET", LevelBucket, "list-type=2&prefix=a", nil, OpListObjectsV2},
		{"list-type=3 is v1", "GET", LevelBucket, "list-type=3", nil, OpListObjects},
		{"plain bucket GET is v1", "GET", LevelBucket, "prefix=a&delimiter=/", nil, OpListObjects},
		{"analytics with id is Get", "GET", LevelBucket, "analytics&id=x", nil, OpGetBucketAnalyticsConfiguration},
		{"analytics without id is List", "GET", LevelBucket, "analytics", nil, OpListBucketAnalyticsConfigurations},
		{"policyStatus is not policy", "GET", LevelBucket, "policyStatus", nil, OpGetBucketPolicyStatus},
		{"object-lock is bucket level", "PUT", LevelBucket, "object-lock", nil, OpPutObjectLockConfiguration},
		{"select needs select-type=2", "POST", LevelObject, "select&select-type=1", nil, OpUnknown},
		{"select with select-type=2", "POST", LevelObject, "select&select-type=2", nil, OpSelectObjectContent},
		{"delete on bucket POST", "POST", LevelBucket, "delete", nil, OpDeleteObjects},
		{"bucket POST without delete is PostObject", "POST", LevelBucket, "", nil, OpPostObject},
		{"object-level uploads is Unknown", "GET", LevelObject, "uploads", nil, OpUnknown},
		{"object-level versions is Unknown", "GET", LevelObject, "versions", nil, OpUnknown},
		{"OPTIONS is preflight", "OPTIONS", LevelObject, "", nil, OpPreflight},
		{"PATCH is unknown", "PATCH", LevelBucket, "", nil, OpUnknown},
		{"service PUT is unknown", "PUT", LevelService, "", nil, OpUnknown},
		{"tagging on object GET", "GET", LevelObject, "tagging", nil, OpGetObjectTagging},
		{"tagging on bucket DELETE", "DELETE", LevelBucket, "tagging", nil, OpDeleteBucketTagging},
		{"HEAD bucket", "HEAD", LevelBucket, "acl", nil, OpHeadBucket},
		{"HEAD object ignores query", "HEAD", LevelObject, "versionId=1&partNumber=2", nil, OpHeadObject},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := url.ParseQuery(c.query)
			if err != nil {
				t.Fatal(err)
			}
			h := http.Header{}
			for k, v := range c.header {
				h.Set(k, v)
			}
			if got := classify(c.method, c.level, q, h); got != c.want {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}

func TestOpNamesComplete(t *testing.T) {
	for o := Op(0); o < opCount; o++ {
		if opNames[o] == "" {
			t.Errorf("op %d has no name", o)
		}
		if o != OpUnknown && o.String() == "Unknown" {
			t.Errorf("op %d stringifies as Unknown", o)
		}
	}
	if opCount.String() != "Unknown" || Op(9999).String() != "Unknown" {
		t.Error("out-of-range ops must stringify as Unknown")
	}
	seen := map[Op]bool{}
	for _, r := range rules {
		seen[r.op] = true
	}
	var missing []string
	for o := OpListBuckets; o < opCount; o++ {
		if !seen[o] {
			missing = append(missing, o.String())
		}
	}
	if len(missing) > 0 {
		t.Errorf("ops with no classifier row: %s", strings.Join(missing, ", "))
	}
}

func TestIsData(t *testing.T) {
	for _, o := range []Op{OpGetObject, OpPutObject, OpUploadPart, OpPostObject, OpSelectObjectContent, OpCopyObject, OpUploadPartCopy, OpUnknown} {
		if !o.IsData() {
			t.Errorf("%s should be a data op", o)
		}
	}
	for _, o := range []Op{OpHeadObject, OpListObjectsV2, OpDeleteObject, OpCreateMultipartUpload, OpCompleteMultipartUpload, OpListBuckets} {
		if o.IsData() {
			t.Errorf("%s should be a metadata op", o)
		}
	}
}

func BenchmarkClassify(b *testing.B) {
	q := url.Values{"uploadId": {"1"}, "partNumber": {"7"}}
	h := http.Header{}
	b.ReportAllocs()
	for b.Loop() {
		if classify(http.MethodPut, LevelObject, q, h) != OpUploadPart {
			b.Fatal("misclassified")
		}
	}
}

func BenchmarkClassifyGetObject(b *testing.B) {
	q := url.Values{}
	h := http.Header{}
	b.ReportAllocs()
	for b.Loop() {
		if classify(http.MethodGet, LevelObject, q, h) != OpGetObject {
			b.Fatal("misclassified")
		}
	}
}
