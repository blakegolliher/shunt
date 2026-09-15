package s3

import (
	"net/http/httptest"
	"strings"
	"testing"
)

var testDomains = NewDomains([]string{"*.shunt.example.com", "*.s3.example.net"})

func req(method, host, target string) RequestInfo {
	r := httptest.NewRequest(method, target, nil)
	r.Host = host
	return Parse(r, testDomains)
}

func TestParseStyles(t *testing.T) {
	cases := []struct {
		name         string
		method, host string
		target       string
		bucket, key  string
		style        Style
		level        Level
		op           Op
	}{
		{"path bucket", "GET", "shunt.example.com:8443", "/b?list-type=2", "b", "", StylePath, LevelBucket, OpListObjectsV2},
		{"path object", "GET", "shunt.example.com", "/b/dir/k.txt", "b", "dir/k.txt", StylePath, LevelObject, OpGetObject},
		{"path bucket trailing slash", "PUT", "shunt.example.com", "/b/", "b", "", StylePath, LevelBucket, OpCreateBucket},
		{"path service", "GET", "shunt.example.com", "/", "", "", StylePath, LevelService, OpListBuckets},
		{"vhost bucket", "GET", "b.shunt.example.com:8443", "/", "b", "", StyleVirtualHost, LevelBucket, OpListObjects},
		{"vhost object", "PUT", "b.shunt.example.com", "/k", "b", "k", StyleVirtualHost, LevelObject, OpPutObject},
		{"vhost dotted bucket", "HEAD", "my.bucket.shunt.example.com", "/", "my.bucket", "", StyleVirtualHost, LevelBucket, OpHeadBucket},
		{"vhost second domain", "GET", "x.s3.example.net", "/k?uploadId=1", "x", "k", StyleVirtualHost, LevelObject, OpListParts},
		{"unknown host is path style", "GET", "127.0.0.1:3900", "/b/k", "b", "k", StylePath, LevelObject, OpGetObject},
		{"escaped key", "GET", "shunt.example.com", "/b/a%20b/c%2Fd", "b", "a b/c/d", StylePath, LevelObject, OpGetObject},
		{"plus stays literal", "GET", "shunt.example.com", "/b/a+b", "b", "a+b", StylePath, LevelObject, OpGetObject},
		{"double slash key", "GET", "shunt.example.com", "/b//k", "b", "/k", StylePath, LevelObject, OpGetObject},
		{"trailing slash key", "PUT", "shunt.example.com", "/b/dir/", "b", "dir/", StylePath, LevelObject, OpPutObject},
		{"upper-case host", "GET", "B.SHUNT.EXAMPLE.COM", "/", "b", "", StyleVirtualHost, LevelBucket, OpListObjects},
		{"escaped slash in bucket stays raw", "GET", "shunt.example.com", "/a%2Fb/k", "a%2Fb", "k", StylePath, LevelObject, OpGetObject},
		{"empty bucket segment is service level", "PUT", "shunt.example.com", "//k", "", "", StylePath, LevelService, OpUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := req(c.method, c.host, c.target)
			if got.Bucket != c.bucket || got.Key != c.key || got.Style != c.style || got.Level != c.level || got.Op != c.op {
				t.Fatalf("got bucket=%q key=%q style=%s level=%s op=%s; want bucket=%q key=%q style=%s level=%s op=%s",
					got.Bucket, got.Key, got.Style, got.Level, got.Op, c.bucket, c.key, c.style, c.level, c.op)
			}
		})
	}
}

func TestParseLongKey(t *testing.T) {
	key := strings.Repeat("k", 1024)
	got := req("GET", "shunt.example.com", "/b/"+key)
	if got.Key != key || got.Level != LevelObject {
		t.Fatalf("long key not preserved: len=%d level=%s", len(got.Key), got.Level)
	}
}

func FuzzParse(f *testing.F) {
	f.Add("GET", "b.shunt.example.com", "/k?uploadId=1")
	f.Add("PUT", "shunt.example.com:8443", "/b/k%2F%20?partNumber=1&uploadId=x")
	f.Add("POST", "127.0.0.1", "/b?delete")
	f.Add("GET", "", "//")
	f.Add("OPTIONS", "b.s3.example.net", "/%zz")
	f.Add("GET", "", "/0%2F0") // an escaped slash in the bucket segment must not decode into the bucket
	f.Add("PUT", "", "//%20")  // empty bucket segment is service level, not an object with no bucket
	f.Fuzz(func(t *testing.T, method, host, target string) {
		if !strings.HasPrefix(target, "/") || strings.ContainsAny(target, " \r\n") || strings.ContainsAny(host, " /\r\n") {
			return
		}
		r := httptest.NewRequest("GET", "/", nil)
		r.Method = method
		r.Host = host
		if u, err := r.URL.Parse(target); err == nil {
			r.URL = u
		} else {
			return
		}
		info := Parse(r, testDomains)
		if strings.Contains(info.Bucket, "/") {
			t.Fatalf("bucket contains '/': %q from %q", info.Bucket, target)
		}
		if info.Level == LevelService && (info.Bucket != "" || info.Key != "") {
			t.Fatalf("service level with bucket/key: %+v", info)
		}
		if info.Level == LevelBucket && info.Key != "" {
			t.Fatalf("bucket level with key: %+v", info)
		}
		if info.Level == LevelObject && (info.Bucket == "" || info.Key == "") {
			t.Fatalf("object level missing bucket or key: %+v", info)
		}
		_ = info.Op.String()
	})
}

func BenchmarkParse(b *testing.B) {
	r := httptest.NewRequest("PUT", "/k?partNumber=3&uploadId=abc", nil)
	r.Host = "bucket.shunt.example.com:8443"
	b.ReportAllocs()
	for b.Loop() {
		if Parse(r, testDomains).Op != OpUploadPart {
			b.Fatal("misparsed")
		}
	}
}
