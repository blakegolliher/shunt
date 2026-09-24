package proxy

// POC-3 handler tests: one endpoint over two signature-verifying fake clusters.

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

const (
	acmeAK, acmeSK = "ACMECLIENTKEY", "acme-client-secret"
	zedAK, zedSK   = "ZEDCLIENTKEY", "zed-client-secret"
)

// fakeS3 is a tiny path-style S3 backend that verifies every request's signature with its own
// cluster key, keeps buckets and objects in memory, and echoes bucket names, its Host, and its
// upload ids the way MinIO does.
type fakeS3 struct {
	name, ak, secret string
	t                testing.TB

	mu       sync.Mutex
	buckets  map[string]map[string][]byte
	uploads  map[string]string // upload id → bucket/key
	parts    map[string][][]byte
	seen     []string // "METHOD RequestURI"
	headers  []http.Header
	nextID   int
	foreign  map[string]bool // CreateBucket answers BucketAlreadyExists
	deny     map[string]bool // CreateBucket answers AccessDenied
	redirect bool
	listPad  int  // extra <Contents> entries in listings
	escapes  bool // percent-encodes "/" in listing keys, as Garage 2.3.0 does
	// vastAfter resumes a delimited listing as VAST 5.5 does (docs/reference/backend-compat.md):
	// start-after=<prefix> skips the prefix, and <prefix>+U+10FFFF lists it again.
	vastAfter bool
	cutList   int // > 0: listings promise a large Content-Length, send this many bytes, and die

	ignoreINM     bool                 // accepts If-None-Match: * and overwrites anyway, as Garage 2.3.0 does
	ignoreIfMatch bool                 // deletes whatever If-Match says, as a backend without conditional deletes does
	noHistory     bool                 // keep no per-request record (seen, headers): long runs would hold every request
	observe       func(backendEvent)   // called for every object request, with the lock held
	before        func(*http.Request)  // called before a request is served, without the lock; set it under the lock
	deleteStatus  int                  // > 0: every object DELETE fails with this status
	mtime         map[string]time.Time // bucket/key -> when the object was last written
	layout        map[string][][]byte  // bucket/key -> the parts it was written as, for a multipart object
}

// etagAt is the ETag the fake serves for one object: a multipart object carries the "-N" suffix S3
// gives it, so a copy that keeps the part layout keeps the ETag's shape.
func (f *fakeS3) etagAt(bucket, key string, data []byte) string {
	if parts := f.layout[bucket+"/"+key]; len(parts) > 1 {
		return fmt.Sprintf(`%s-%d"`, strings.TrimSuffix(etagOf(data), `"`), len(parts))
	}
	return etagOf(data)
}

// putMultipart seeds an object that was written as several parts, as a client's multipart upload
// leaves it.
func (f *fakeS3) putMultipart(t testing.TB, bucket, key string, parts [][]byte) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.buckets[bucket] == nil {
		t.Fatalf("no bucket %s", bucket)
	}
	f.buckets[bucket][key] = bytes.Join(parts, nil)
	f.layout[bucket+"/"+key] = parts
	f.mtime[bucket+"/"+key] = time.Now()
}

// partsOf is the part layout the fake holds for an object, for a test to compare across clusters.
func (f *fakeS3) partsOf(bucket, key string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.layout[bucket+"/"+key]
}

// etagOf is the fake's ETag for an object: the hex MD5 of its bytes, quoted, as S3 gives a
// single-part object.
func etagOf(data []byte) string {
	sum := md5.Sum(data) //nolint:gosec // S3 ETag, not security
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// wrote records an object write for Last-Modified. Called with the lock held.
func (f *fakeS3) wrote(bucket, key string) { f.mtime[bucket+"/"+key] = time.Now() }

// backendEvent is one object request as the backend saw it, for the property test's record.
type backendEvent struct {
	start, end  time.Time
	method, key string
	bucket      string
	status      int
	ifNoneMatch bool
	body        []byte // the PUT body, or the GET body on a 200
	opID        string // X-Property-Op: the client op or mover step that sent it
}

// statusWriter remembers the status a fake backend answered with.
type statusWriter struct {
	http.ResponseWriter
	status int
	body   []byte
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	if len(s.body) < 256 {
		s.body = append(s.body, p[:min(len(p), 256-len(s.body))]...)
	}
	return s.ResponseWriter.Write(p)
}

func newFakeS3(t testing.TB, name, ak, secret string, buckets ...string) *fakeS3 {
	f := &fakeS3{name: name, ak: ak, secret: secret, t: t, buckets: map[string]map[string][]byte{}, uploads: map[string]string{},
		parts: map[string][][]byte{}, foreign: map[string]bool{}, deny: map[string]bool{}, mtime: map[string]time.Time{}, layout: map[string][][]byte{}}
	for _, b := range buckets {
		f.buckets[b] = map[string][]byte{}
	}
	return f
}

func (f *fakeS3) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.seen) }

func (f *fakeS3) last() (string, http.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seen) == 0 {
		return "", nil
	}
	return f.seen[len(f.seen)-1], f.headers[len(f.headers)-1]
}

// hasBucket reports whether the backend holds this bucket.
func (f *fakeS3) hasBucket(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.buckets[name]
	return ok
}

// dropBucket removes a bucket behind the backend's back, as if it had vanished.
func (f *fakeS3) dropBucket(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.buckets, name)
}

// addUpload registers an upload id the way a backend that predates the codec would have.
func (f *fakeS3) addUpload(id, bucketKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads[id] = bucketKey
}

func (f *fakeS3) setListPad(n int) { f.mu.Lock(); defer f.mu.Unlock(); f.listPad = n }
func (f *fakeS3) setCut(n int)     { f.mu.Lock(); defer f.mu.Unlock(); f.cutList = n }
func (f *fakeS3) setRedirect()     { f.mu.Lock(); defer f.mu.Unlock(); f.redirect = true }

func (f *fakeS3) object(bucket, key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.buckets[bucket][key]
	return b, ok
}

func (f *fakeS3) addBucket(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buckets[name] = map[string][]byte{}
}

// setForeign makes CreateBucket answer BucketAlreadyExists for this name; setDeny makes it 403.
func (f *fakeS3) setForeign(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.foreign[name] = true
}

func (f *fakeS3) setDeny(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deny[name] = true
}

func (f *fakeS3) fail(w http.ResponseWriter, r *http.Request, status int, code, bucket, key string) {
	resource := "/" + bucket
	if key != "" {
		resource += "/" + key
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<Error><Code>%s</Code><Message>%s for %s at %s</Message><Key>%s</Key><BucketName>%s</BucketName><Resource>%s</Resource><RequestId>R</RequestId><HostId>%s</HostId></Error>`,
		code, code, bucket, r.Host, esc(key), bucket, esc(resource), f.name)
}

// parseTestRange reads "bytes=a-b" for the fake's ranged reads.
func parseTestRange(rng string, size int) (first, last int, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(rng), "bytes=")
	if !found {
		return 0, 0, false
	}
	a, b, sep := strings.Cut(spec, "-")
	first, err := strconv.Atoi(strings.TrimSpace(a))
	if err != nil || first >= size {
		return 0, 0, false
	}
	last = size - 1
	if sep && strings.TrimSpace(b) != "" {
		if last, err = strconv.Atoi(strings.TrimSpace(b)); err != nil {
			return 0, 0, false
		}
	}
	if last >= size {
		last = size - 1
	}
	return first, last, last >= first
}

func esc(s string) string {
	var b strings.Builder
	_ = xmlEscapeTo(&b, s)
	return b.String()
}

func xmlEscapeTo(w io.Writer, s string) error {
	_, err := io.WriteString(w, strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s))
	return err
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	store := mapStore{f.ak: {AccessKey: f.ak, Secret: f.secret, Tenant: "cluster"}}
	if _, aerr := sigv4.Verify(r.Context(), r, store, time.Now(), sigv4.Options{RequireHash: true}); aerr != nil {
		f.t.Errorf("%s: upstream request not signed with the cluster key: %v (%s %s)", f.name, aerr, r.Method, r.RequestURI)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	before := f.before
	f.mu.Unlock()
	if before != nil {
		before(r)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.observe != nil {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		w = sw
		defer func() {
			bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
			if key == "" {
				return
			}
			ev := backendEvent{start: start, end: time.Now(), method: r.Method, bucket: bucket, key: key, status: sw.status,
				ifNoneMatch: r.Header.Get("If-None-Match") == "*", opID: r.Header.Get("X-Property-Op")}
			if ev.status == 0 {
				ev.status = http.StatusOK
			}
			switch {
			case r.Method == http.MethodPut:
				ev.body = body
			case r.Method == http.MethodGet && ev.status == http.StatusOK:
				ev.body = sw.body
			}
			f.observe(ev)
		}()
	}
	if !f.noHistory {
		f.seen = append(f.seen, r.Method+" "+r.RequestURI)
		f.headers = append(f.headers, r.Header.Clone())
	}
	w.Header().Set("X-Amz-Request-Id", f.name+"-req")
	if f.redirect {
		w.Header().Set("Location", "http://elsewhere.example"+r.RequestURI)
		w.Header().Set("X-Amz-Bucket-Region", "eu-west-9")
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	objs, exists := f.buckets[bucket]
	switch {
	case key == "" && r.Method == http.MethodPut && len(q) == 0:
		switch {
		case f.foreign[bucket]:
			f.fail(w, r, 409, "BucketAlreadyExists", bucket, "")
		case f.deny[bucket]:
			f.fail(w, r, 403, "AccessDenied", bucket, "")
		case exists:
			f.fail(w, r, 409, "BucketAlreadyOwnedByYou", bucket, "")
		default:
			f.buckets[bucket] = map[string][]byte{}
			w.Header().Set("Location", "/"+bucket)
		}
		return
	case !exists:
		f.fail(w, r, 404, "NoSuchBucket", bucket, key)
		return
	case key == "" && r.Method == http.MethodDelete:
		if len(objs) > 0 {
			f.fail(w, r, 409, "BucketNotEmpty", bucket, "")
			return
		}
		delete(f.buckets, bucket)
		w.WriteHeader(http.StatusNoContent)
	case key == "" && r.Method == http.MethodHead:
	case key == "" && q.Has("policy"):
		_, _ = w.Write([]byte(`{"Version":"2012-10-17","Statement":[]}`))
	case key == "" && q.Has("versioning"):
		_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)) // never versioned, as S3 answers it
	case key == "" && q.Has("uploads"):
		fmt.Fprintf(w, `<ListMultipartUploadsResult><Bucket>%s</Bucket><UploadIdMarker></UploadIdMarker>`, bucket)
		ids := make([]string, 0, len(f.uploads))
		for id := range f.uploads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Fprintf(w, `<Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>`, esc(strings.SplitN(f.uploads[id], "/", 2)[1]), id)
		}
		_, _ = w.Write([]byte(`</ListMultipartUploadsResult>`))
	case key == "" && r.Method == http.MethodGet:
		// A listing that honors prefix, delimiter, start-after, continuation-token and max-keys,
		// which the merge in internal/proxy/merge.go depends on.
		prefix, delim := q.Get("prefix"), q.Get("delimiter")
		after := q.Get("start-after")
		if f.vastAfter && delim != "" {
			switch {
			case strings.HasSuffix(after, "\U0010FFFF"):
				after = strings.TrimSuffix(strings.TrimSuffix(after, "\U0010FFFF"), delim) // before the prefix
			case strings.HasSuffix(after, delim):
				after += "\U0010FFFF" // past everything under the prefix
			}
		}
		if t := q.Get("continuation-token"); t != "" {
			after = t
		}
		maxKeys := 1000
		if v := q.Get("max-keys"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				maxKeys = n
			}
		}
		keys := make([]string, 0, len(objs)+f.listPad)
		for k := range objs {
			keys = append(keys, k)
		}
		for i := 0; i < f.listPad; i++ {
			keys = append(keys, fmt.Sprintf("pad/%06d-%s", i, bucket))
		}
		sort.Strings(keys)
		var contents []string
		seenPrefix, order := map[string]bool{}, []string{}
		truncated, last := false, ""
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) || (after != "" && k <= after) {
				continue
			}
			if delim != "" {
				if i := strings.Index(k[len(prefix):], delim); i >= 0 {
					cp := k[:len(prefix)+i+len(delim)]
					if !seenPrefix[cp] {
						if len(contents)+len(order) >= maxKeys {
							truncated = true
							break
						}
						seenPrefix[cp], order = true, append(order, cp)
					}
					last = k
					continue
				}
			}
			if len(contents)+len(order) >= maxKeys {
				truncated = true
				break
			}
			contents, last = append(contents, k), k
		}
		var b bytes.Buffer
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`,
			bucket, esc(prefix), len(contents)+len(order), maxKeys, truncated)
		if truncated {
			fmt.Fprintf(&b, `<NextContinuationToken>%s</NextContinuationToken>`, esc(last))
		}
		enc := func(k string) string {
			if f.escapes {
				return strings.ReplaceAll(k, "/", "%2F")
			}
			return k
		}
		for _, k := range contents {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>2026-09-15T18:00:00.000Z</LastModified><ETag>&quot;%s&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`,
				esc(enc(k)), f.name, len(objs[k]))
		}
		for _, cp := range order {
			fmt.Fprintf(&b, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, esc(enc(cp)))
		}
		b.WriteString(`</ListBucketResult>`)
		if f.cutList > 0 {
			w.Header().Set("Content-Length", "1000000")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b.Bytes()[:min(f.cutList, b.Len())])
			http.NewResponseController(w).Flush() //nolint:errcheck // test
			if c, _, err := http.NewResponseController(w).Hijack(); err == nil {
				c.Close()
			}
			return
		}
		_, _ = w.Write(b.Bytes())
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.nextID++
		id := fmt.Sprintf("%s-up-%d", f.name, f.nextID)
		f.uploads[id] = bucket + "/" + key
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, bucket, esc(key), id)
	case q.Has("uploadId"):
		id := q.Get("uploadId")
		if f.uploads[id] != bucket+"/"+key {
			f.fail(w, r, 404, "NoSuchUpload", bucket, key)
			return
		}
		switch r.Method {
		case http.MethodPut:
			f.parts[id] = append(f.parts[id], body)
			w.Header().Set("ETag", `"part"`)
		case http.MethodGet:
			fmt.Fprintf(w, `<ListPartsResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId><Part><PartNumber>1</PartNumber></Part></ListPartsResult>`, bucket, esc(key), id)
		case http.MethodPost:
			objs[key] = bytes.Join(f.parts[id], nil)
			f.layout[bucket+"/"+key] = f.parts[id]
			f.wrote(bucket, key)
			delete(f.uploads, id)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<CompleteMultipartUploadResult><Location>http://%s/%s/%s</Location><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`,
				r.Host, bucket, esc(key), bucket, esc(key), esc(f.etagAt(bucket, key, objs[key])))
		case http.MethodDelete:
			delete(f.uploads, id)
			w.WriteHeader(http.StatusNoContent)
		}
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		src, _ := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/"))
		sb, sk, _ := strings.Cut(src, "/")
		data, ok := f.buckets[sb][sk]
		if !ok {
			f.fail(w, r, 404, "NoSuchKey", sb, sk)
			return
		}
		objs[key] = data
		f.wrote(bucket, key)
		_, _ = w.Write([]byte(`<CopyObjectResult><ETag>"copy"</ETag></CopyObjectResult>`))
	case r.Method == http.MethodPut:
		// Conditional write, as a backend with capabilities.conditional_write=true offers it: the
		// mover uses it so a client write during the copy is never clobbered (ADR-0004).
		if _, taken := objs[key]; taken && r.Header.Get("If-None-Match") == "*" && !f.ignoreINM {
			f.fail(w, r, 412, "PreconditionFailed", bucket, key)
			return
		}
		// If-Match on a write, as a backend that offers conditional updates judges it: against the
		// copy this backend holds, which is why shunt judges it across both clusters (ADR-0013).
		if im := r.Header.Get("If-Match"); im != "" {
			cur, taken := objs[key]
			if !taken || !etagMatches(im, etagOf(cur)) {
				f.fail(w, r, 412, "PreconditionFailed", bucket, key)
				return
			}
		}
		objs[key] = body
		delete(f.layout, bucket+"/"+key) // a plain PUT replaces a multipart object with one part
		f.wrote(bucket, key)
		w.Header().Set("ETag", etagOf(body))
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		data, ok := objs[key]
		if !ok {
			f.fail(w, r, 404, "NoSuchKey", bucket, key)
			return
		}
		parts := f.layout[bucket+"/"+key]
		etag := f.etagAt(bucket, key, data)
		// Read conditions, as a backend evaluates them against the copy it holds.
		if im := r.Header.Get("If-Match"); im != "" && !etagMatches(im, etag) {
			f.fail(w, r, 412, "PreconditionFailed", bucket, key)
			return
		}
		if inm := r.Header.Get("If-None-Match"); inm != "" && etagMatches(inm, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", f.mtime[bucket+"/"+key].UTC().Format(http.TimeFormat))
		if len(parts) > 1 {
			w.Header().Set("X-Amz-Mp-Parts-Count", fmt.Sprint(len(parts)))
		}
		// A request for one part of a multipart object answers that part alone, as S3 does.
		if n := q.Get("partNumber"); n != "" && len(parts) > 0 {
			i, err := strconv.Atoi(n)
			if err != nil || i < 1 || i > len(parts) {
				f.fail(w, r, 416, "InvalidPartNumber", bucket, key)
				return
			}
			data = parts[i-1]
		}
		if rng := r.Header.Get("Range"); rng != "" {
			if a, b, ok := parseTestRange(rng, len(data)); ok {
				data = data[a : b+1]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(objs[key])))
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data)
				return
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data)
	case r.Method == http.MethodDelete && f.deleteStatus > 0:
		f.fail(w, r, f.deleteStatus, "ServiceUnavailable", bucket, key)
	case r.Method == http.MethodDelete:
		// Conditional delete, as a backend that supports If-Match on DeleteObject offers it: the
		// mover uses it to take back only its own copy (ADR-0004 race 1).
		if want := r.Header.Get("If-Match"); want != "" && !f.ignoreIfMatch {
			data, ok := objs[key]
			switch {
			case !ok:
				f.fail(w, r, 404, "NoSuchKey", bucket, key)
				return
			case etagOf(data) != want:
				f.fail(w, r, 412, "PreconditionFailed", bucket, key)
				return
			}
		}
		delete(objs, key)
		delete(f.mtime, bucket+"/"+key)
		w.WriteHeader(http.StatusNoContent)
	}
}

const mixedDirectory = `version: 1
tenants: { acme: { default_cluster: garage }, zed: { default_cluster: minio } }
placements:
  acme/data: { state: ACTIVE, primary: garage, names: { garage: acme-1111-data }, created: 2026-09-15T18:00:00Z }
  acme/pics: { state: ACTIVE, primary: garage, names: { garage: acme-7777-pics } }
  acme/old:  { state: ACTIVE, primary: minio,  names: { minio: acme-2222-old }, created: 2026-09-01T00:00:00Z }
  zed/data:  { state: ACTIVE, primary: minio,  names: { minio: zed-3333-data } }
  zed/logs:  { state: ACTIVE, primary: minio,  names: { minio: zed-4444-logs } }
`

// writeDirectory writes a directory file: body (starting with its version line) with clusters,
// which are directory state since POC-5.
func writeDirectory(t testing.TB, path, body string, clusters map[string]config.Cluster) {
	t.Helper()
	block, err := yaml.Dump(map[string]map[string]config.Cluster{"clusters": clusters}, yaml.WithIndent(2))
	if err != nil {
		t.Fatal(err)
	}
	version, rest, _ := strings.Cut(body, "\n")
	if err := os.WriteFile(path, []byte(version+"\n"+string(block)+rest), 0o644); err != nil {
		t.Fatal(err)
	}
}

// rigDebugRoute turns features.debug_route_header on for rigs built while it is set. Proxy tests
// do not run in parallel.
var rigDebugRoute bool

type mixedRig struct {
	front             *httptest.Server
	h                 *Handler
	garage, minio     *fakeS3
	gAddr, mAddr      string
	dir               *directory.FileDir
	dirPath           string
	clusters          map[string]config.Cluster
	accessLog, alerts *syncBuf
	backendNames      []string
}

func newMixedRig(t testing.TB, store mapStore, adjust ...func(map[string]config.Cluster)) *mixedRig {
	t.Helper()
	m := &mixedRig{accessLog: &syncBuf{}, alerts: &syncBuf{}}
	m.garage = newFakeS3(t, "garage", "GARAGECLUSTERKEY", "garage-cluster-secret", "acme-1111-data", "acme-7777-pics")
	m.minio = newFakeS3(t, "minio", "MINIOCLUSTERKEY", "minio-cluster-secret", "acme-2222-old", "zed-3333-data", "zed-4444-logs")
	gs, ms := httptest.NewServer(m.garage), httptest.NewServer(m.minio)
	t.Cleanup(gs.Close)
	t.Cleanup(ms.Close)
	m.gAddr, m.mAddr = strings.TrimPrefix(gs.URL, "http://"), strings.TrimPrefix(ms.URL, "http://")
	m.backendNames = []string{"acme-1111-data", "acme-7777-pics", "acme-2222-old", "zed-3333-data", "zed-4444-logs"}
	m.clusters = map[string]config.Cluster{
		"garage": {Type: "s3", Scheme: "http", Region: "garage", EndpointMode: "static", Endpoints: []string{m.gAddr},
			Credentials: config.Credentials{AccessKey: "GARAGECLUSTERKEY", SecretRef: "env:G"}},
		"minio": {Type: "minio", Scheme: "http", Region: "us-east-1", EndpointMode: "static", Endpoints: []string{m.mAddr},
			Credentials: config.Credentials{AccessKey: "MINIOCLUSTERKEY", SecretRef: "env:M"}},
	}
	for _, f := range adjust {
		f(m.clusters)
	}
	secrets := map[string]string{"env:G": "garage-cluster-secret", "env:M": "minio-cluster-secret"}
	set := upstream.NewRegistry(upstream.Options{DialTimeout: time.Second}, func(ref string) (string, error) { return secrets[ref], nil })
	t.Cleanup(set.Close)
	m.dirPath = filepath.Join(t.TempDir(), "directory.yaml")
	writeDirectory(t, m.dirPath, mixedDirectory, m.clusters)
	var err error
	if m.dir, err = directory.Open(m.dirPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := set.Apply(m.dir.Snapshot().File().Clusters); err != nil {
		t.Fatal(err)
	}
	m.dir.Prepare = func(f *directory.File, resolve func(string) (string, error)) (func(), error) {
		cand, err := set.Prepare(f.Clusters, resolve)
		if err != nil {
			return nil, err
		}
		return func() { cand.Commit() }, nil
	}
	if store == nil {
		store = mapStore{acmeAK: {AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"}, zedAK: {AccessKey: zedAK, Secret: zedSK, Tenant: "zed"}}
	}
	m.h = New(Handler{
		Mode: ModeResign, Store: store, Clusters: set, Dir: m.dir, Rewrite: true, DebugRoute: rigDebugRoute,
		Domains: s3.NewDomains([]string{"*.shunt.example.com"}), Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(m.accessLog),
		Slow: telemetry.NewSlowRing(10, time.Hour), IdleTimeout: 2 * time.Second, MetadataTimeout: 5 * time.Second, Via: "1.1 shunt/test",
		Log: slog.New(slog.NewJSONHandler(m.alerts, nil)),
	}, 64<<10)
	m.front = httptest.NewServer(m.h)
	m.front.Config.ErrorLog = nil
	t.Cleanup(m.front.Close)
	return m
}

type reply struct {
	*http.Response
	body []byte
}

// send signs a client request with ak/sk (us-east-1, UNSIGNED-PAYLOAD) and sends it to shunt.
// host "" is path-style against the front server's address.
func (m *mixedRig) send(t testing.TB, method, target, host string, body []byte, ak, sk string, hdr map[string]string) reply {
	t.Helper()
	req, err := http.NewRequest(method, m.front.URL+target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	}
	if host != "" {
		req.Host = host
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: ak, Secret: sk}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fresh().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return reply{resp, b}
}

func (m *mixedRig) acme(t testing.TB, method, target string, body []byte) reply {
	t.Helper()
	return m.send(t, method, target, "", body, acmeAK, acmeSK, nil)
}

func (m *mixedRig) upstreamCalls() int { return m.garage.count() + m.minio.count() }

// noLeak fails when a response header or body names a backend bucket or a cluster endpoint.
func (m *mixedRig) noLeak(t *testing.T, what string, r reply) {
	t.Helper()
	var all strings.Builder
	for k, vv := range r.Header {
		all.WriteString(k + ": " + strings.Join(vv, ",") + "\n")
	}
	all.Write(r.body)
	s := all.String()
	for _, n := range append(m.backendNames, m.gAddr, m.mAddr) {
		if strings.Contains(s, n) {
			t.Errorf("%s leaks %q:\n%s", what, n, s)
		}
	}
}

func between(b []byte, open, closeTag string) string {
	s := string(b)
	i := strings.Index(s, open)
	if i < 0 {
		return ""
	}
	s = s[i+len(open):]
	j := strings.Index(s, closeTag)
	if j < 0 {
		return ""
	}
	return s[:j]
}

// TestTenantIsolation is the POC-3 acceptance proof: the same bucket name under two tenants on two
// different clusters never crosses, and no tenant reaches another tenant's bucket or a backend
// bucket by its backend name.
func TestTenantIsolation(t *testing.T) {
	m := newMixedRig(t, nil)
	if r := m.send(t, "PUT", "/data/k", "", []byte("from acme"), acmeAK, acmeSK, nil); r.StatusCode != 200 {
		t.Fatalf("acme put: %d %s", r.StatusCode, r.body)
	}
	if r := m.send(t, "PUT", "/k", "data.shunt.example.com", []byte("from zed"), zedAK, zedSK, nil); r.StatusCode != 200 {
		t.Fatalf("zed put (virtual-host): %d %s", r.StatusCode, r.body)
	}
	if b, ok := m.garage.object("acme-1111-data", "k"); !ok || string(b) != "from acme" {
		t.Fatalf("acme's object not on garage under its backend name: %q %v", b, ok)
	}
	if b, ok := m.minio.object("zed-3333-data", "k"); !ok || string(b) != "from zed" {
		t.Fatalf("zed's object not on minio under its backend name: %q %v", b, ok)
	}
	if r := m.acme(t, "GET", "/data/k", nil); string(r.body) != "from acme" {
		t.Fatalf("acme get: %d %q", r.StatusCode, r.body)
	}
	if r := m.send(t, "GET", "/data/k", "", nil, zedAK, zedSK, nil); string(r.body) != "from zed" {
		t.Fatalf("zed get: %d %q", r.StatusCode, r.body)
	}
	r := m.acme(t, "GET", "/data?list-type=2", nil)
	if r.StatusCode != 200 || !strings.Contains(string(r.body), "<Name>data</Name>") || strings.Count(string(r.body), "<Contents>") != 1 {
		t.Fatalf("acme listing: %s", r.body)
	}
	m.noLeak(t, "acme listing", r)

	// Buckets the tenant does not own are refused without an upstream call. shunt's error echoes
	// the client's own request path, so asking for a backend bucket by its backend name shows that
	// name back: the client's input, not something shunt revealed. Only the other answers are
	// scanned for leaks.
	before := m.upstreamCalls()
	for _, tc := range []struct {
		target string
		scan   bool
	}{{"/logs/k", true}, {"/logs", true}, {"/nope/k", true}, {"/zed-3333-data/k", false}, {"/acme-1111-data/k", false}} {
		r := m.acme(t, "GET", tc.target, nil)
		if r.StatusCode != 404 || !strings.Contains(string(r.body), "<Code>NoSuchBucket</Code>") {
			t.Errorf("acme GET %s: %d %s", tc.target, r.StatusCode, r.body)
		}
		if tc.scan {
			m.noLeak(t, "NoSuchBucket for "+tc.target, r)
		}
	}
	if m.upstreamCalls() != before {
		t.Fatalf("requests for buckets the tenant does not own reached a backend (%d calls)", m.upstreamCalls()-before)
	}
	if r := m.send(t, "DELETE", "/data/k", "", nil, zedAK, zedSK, nil); r.StatusCode != 204 {
		t.Fatalf("zed delete: %d", r.StatusCode)
	}
	if r := m.acme(t, "GET", "/data/k", nil); string(r.body) != "from acme" {
		t.Fatalf("zed's delete reached acme's object: %d %q", r.StatusCode, r.body)
	}
}

func TestListBucketsSpansClustersAndHonorsAllowlist(t *testing.T) {
	m := newMixedRig(t, mapStore{
		acmeAK:        {AccessKey: acmeAK, Secret: acmeSK, Tenant: "acme"},
		"ACMELIMITED": {AccessKey: "ACMELIMITED", Secret: "limited", Tenant: "acme", Buckets: []string{"old"}},
	})
	before := m.upstreamCalls()
	r := m.acme(t, "GET", "/", nil)
	names := strings.Split(strings.TrimSuffix(strings.ReplaceAll(between(r.body, "<Buckets>", "</Buckets>"), "<Bucket><Name>", ""), "</Bucket>"), "</Bucket>")
	body := string(r.body)
	if r.StatusCode != 200 || !strings.Contains(body, "<Name>data</Name><CreationDate>2026-09-15T18:00:00.000Z</CreationDate>") ||
		!strings.Contains(body, "<Name>old</Name>") || !strings.Contains(body, "<Name>pics</Name>") || strings.Contains(body, "logs") {
		t.Fatalf("ListBuckets: %s", body)
	}
	if strings.Index(body, "<Name>data</Name>") > strings.Index(body, "<Name>old</Name>") {
		t.Fatalf("buckets not sorted: %v", names)
	}
	if !strings.Contains(body, "<ID>"+ownerID("acme")+"</ID><DisplayName>acme</DisplayName>") || r.ContentLength != int64(len(r.body)) {
		t.Fatalf("owner or framing: %s", body)
	}
	m.noLeak(t, "ListBuckets", r)
	if m.upstreamCalls() != before {
		t.Fatal("ListBuckets reached a backend")
	}
	if r := m.acme(t, "GET", "/?prefix=o", nil); strings.Contains(string(r.body), "<Name>data</Name>") || !strings.Contains(string(r.body), "<Prefix>o</Prefix>") {
		t.Fatalf("prefix: %s", r.body)
	}
	// Amendment 5: a credential with a buckets list sees only those; without one, all of the tenant's.
	lim := m.send(t, "GET", "/", "", nil, "ACMELIMITED", "limited", nil)
	if strings.Contains(string(lim.body), "<Name>data</Name>") || !strings.Contains(string(lim.body), "<Name>old</Name>") {
		t.Fatalf("allowlisted ListBuckets: %s", lim.body)
	}
	if r := m.send(t, "GET", "/data/k", "", nil, "ACMELIMITED", "limited", nil); r.StatusCode != 404 {
		t.Fatalf("allowlist not enforced on requests: %d", r.StatusCode)
	}
	if r := m.send(t, "GET", "/old?list-type=2", "", nil, "ACMELIMITED", "limited", nil); r.StatusCode != 200 {
		t.Fatalf("allowlisted bucket refused: %d %s", r.StatusCode, r.body)
	}
	if r := m.acme(t, "GET", "/data?list-type=2", nil); r.StatusCode != 200 {
		t.Fatalf("credential without a buckets list refused: %d", r.StatusCode)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_requests_total", `cluster="none",cluster_type="none",op="ListBuckets",status_class="2xx"`); v < 1 {
		t.Fatal("synthesized ListBuckets not counted with cluster=none")
	}
}

func TestCreateBucket(t *testing.T) {
	m := newMixedRig(t, nil)
	r := m.acme(t, "PUT", "/fresh", []byte(`<CreateBucketConfiguration><LocationConstraint>mars-1</LocationConstraint></CreateBucketConfiguration>`))
	backend := directory.BackendName("acme", "fresh", 0)
	if r.StatusCode != 200 || r.Header.Get("Location") != "/fresh" || r.ContentLength != 0 {
		t.Fatalf("create: %d %v %s", r.StatusCode, r.Header, r.body)
	}
	if !m.garage.hasBucket(backend) {
		t.Fatalf("backend bucket %s not created on the tenant's default cluster", backend)
	}
	p, ok := m.dir.Snapshot().Lookup("acme", "fresh")
	if !ok || p.Primary != "garage" || p.Names["garage"] != backend || p.Created.IsZero() {
		t.Fatalf("placement: %+v", p)
	}
	m.noLeak(t, "CreateBucket", r)
	if r := m.acme(t, "PUT", "/fresh/obj", []byte("x")); r.StatusCode != 200 {
		t.Fatalf("put into the new bucket: %d", r.StatusCode)
	}
	if r := m.acme(t, "PUT", "/fresh", nil); r.StatusCode != 409 || !strings.Contains(string(r.body), "BucketAlreadyOwnedByYou") {
		t.Fatalf("second create: %d %s", r.StatusCode, r.body)
	}
	// Another tenant may use the same name: it lands on its own default cluster.
	if r := m.send(t, "PUT", "/fresh", "", nil, zedAK, zedSK, nil); r.StatusCode != 200 {
		t.Fatalf("zed create of the same name: %d %s", r.StatusCode, r.body)
	}
	if !m.minio.hasBucket(directory.BackendName("zed", "fresh", 0)) {
		t.Fatal("zed's bucket not on minio")
	}

	before := m.upstreamCalls()
	if r := m.acme(t, "PUT", "/Bad_Name", nil); r.StatusCode != 400 || !strings.Contains(string(r.body), "InvalidBucketName") {
		t.Fatalf("invalid name: %d %s", r.StatusCode, r.body)
	}
	if m.upstreamCalls() != before {
		t.Fatal("an invalid bucket name reached a backend")
	}

	// A backend name owned by someone else: the next generated name is used.
	m.garage.setForeign(directory.BackendName("acme", "taken", 0))
	if r := m.acme(t, "PUT", "/taken", nil); r.StatusCode != 200 {
		t.Fatalf("create with a taken backend name: %d %s", r.StatusCode, r.body)
	}
	if p, _ := m.dir.Snapshot().Lookup("acme", "taken"); p.Names["garage"] != directory.BackendName("acme", "taken", 1) {
		t.Fatalf("retry name not recorded: %+v", p)
	}

	// The backend refuses: its error is relayed with the name rewritten and the claimed row released.
	m.garage.setDeny(directory.BackendName("acme", "denied", 0))
	r = m.acme(t, "PUT", "/denied", nil)
	if r.StatusCode != 403 || !strings.Contains(string(r.body), "<BucketName>denied</BucketName>") || r.ContentLength != int64(len(r.body)) {
		t.Fatalf("backend refusal: %d %s", r.StatusCode, r.body)
	}
	m.backendNames = append(m.backendNames, directory.BackendName("acme", "denied", 0))
	m.noLeak(t, "CreateBucket refusal", r)
	if _, ok := m.dir.Snapshot().Lookup("acme", "denied"); ok {
		t.Fatal("placement row kept after the backend refused the bucket")
	}
}

func TestCreateBucketReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	m := newMixedRig(t, nil)
	dir := filepath.Dir(m.dirPath)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	before := m.upstreamCalls()
	r := m.acme(t, "PUT", "/fresh", nil)
	if r.StatusCode != 503 || !strings.Contains(string(r.body), "ServiceUnavailable") {
		t.Fatalf("read-only directory: %d %s", r.StatusCode, r.body)
	}
	if m.upstreamCalls() != before {
		t.Fatal("a backend bucket was created although the directory could not be written")
	}
}

func TestDeleteBucket(t *testing.T) {
	m := newMixedRig(t, nil)
	if r := m.acme(t, "PUT", "/tmp", nil); r.StatusCode != 200 {
		t.Fatal(r.StatusCode)
	}
	m.acme(t, "PUT", "/tmp/obj", []byte("x"))
	r := m.acme(t, "DELETE", "/tmp", nil)
	if r.StatusCode != 409 || !strings.Contains(string(r.body), "<BucketName>tmp</BucketName>") || !strings.Contains(string(r.body), "<Resource>/tmp</Resource>") {
		t.Fatalf("delete non-empty: %d %s", r.StatusCode, r.body)
	}
	m.backendNames = append(m.backendNames, directory.BackendName("acme", "tmp", 0))
	m.noLeak(t, "BucketNotEmpty", r)
	if _, ok := m.dir.Snapshot().Lookup("acme", "tmp"); !ok {
		t.Fatal("placement removed although the backend kept the bucket")
	}
	m.acme(t, "DELETE", "/tmp/obj", nil)
	if r := m.acme(t, "DELETE", "/tmp", nil); r.StatusCode != 204 {
		t.Fatalf("delete empty: %d %s", r.StatusCode, r.body)
	}
	if _, ok := m.dir.Snapshot().Lookup("acme", "tmp"); ok {
		t.Fatal("placement kept after the bucket was deleted")
	}
	if r := m.acme(t, "GET", "/tmp/obj", nil); r.StatusCode != 404 || !strings.Contains(string(r.body), "NoSuchBucket") {
		t.Fatalf("deleted bucket still served: %d", r.StatusCode)
	}

	// A placement whose backend bucket vanished: the backend's 404 is relayed and the row removed.
	m.garage.dropBucket("acme-7777-pics")
	if r := m.acme(t, "DELETE", "/pics", nil); r.StatusCode != 404 {
		t.Fatalf("vanished bucket: %d", r.StatusCode)
	}
	if _, ok := m.dir.Snapshot().Lookup("acme", "pics"); ok || !strings.Contains(m.alerts.String(), "already gone") {
		t.Fatalf("vanished bucket's placement kept (alerts %s)", m.alerts.String())
	}

	// Not ACTIVE: DeleteBucket and PutBucketVersioning are refused without an upstream call.
	if err := m.dir.SetState(context.Background(), "acme", "old", directory.StateActive,
		directory.Transition{To: directory.StateMigrating, Target: "garage", Name: "acme-6666-old"}, "test"); err != nil {
		t.Fatal(err)
	}
	before := m.upstreamCalls()
	for _, req := range [][2]string{{"DELETE", "/old"}, {"PUT", "/old?versioning"}} {
		r := m.acme(t, req[0], req[1], []byte{})
		if r.StatusCode != 409 || !strings.Contains(string(r.body), "InvalidBucketState") || !strings.Contains(string(r.body), "MIGRATING") {
			t.Errorf("%s %s on a MIGRATING bucket: %d %s", req[0], req[1], r.StatusCode, r.body)
		}
	}
	if m.upstreamCalls() != before {
		t.Fatal("refused bucket operations reached a backend")
	}
	// A MIGRATING bucket still lists, from whichever side holds the objects (see migration_test.go).
	if r := m.acme(t, "GET", "/old?list-type=2", nil); r.StatusCode != 200 {
		t.Fatalf("MIGRATING listing: %d %s", r.StatusCode, r.body)
	}
}

func (m *mixedRig) garageLast() string { s, _ := m.garage.last(); return s }

// TestUploadIDRoundTripAcrossDirectoryReload: a multipart upload started on one cluster completes
// there after another writer moves the bucket's primary, because the uploadId carries the cluster.
func TestUploadIDRoundTripAcrossDirectoryReload(t *testing.T) {
	m := newMixedRig(t, nil)
	key := "dir/a b+c.bin"
	ekey := "dir/a%20b%2Bc.bin"
	r := m.acme(t, "POST", "/data/"+ekey+"?uploads", nil)
	uid := between(r.body, "<UploadId>", "</UploadId>")
	gid := upstream.ClusterID("garage")
	if r.StatusCode != 200 || !strings.HasPrefix(uid, gid+"~garage-up-") || !strings.Contains(string(r.body), "<Bucket>data</Bucket>") {
		t.Fatalf("initiate: %d %s", r.StatusCode, r.body)
	}
	m.noLeak(t, "InitiateMultipartUpload", r)

	// Another writer moves acme/data to minio; this proxy picks the change up on reload.
	other, err := directory.Open(m.dirPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m.minio.addBucket("acme-5555-data")
	if err := other.SetState(ctx, "acme", "data", directory.StateActive, directory.Transition{To: directory.StateMigrating, Target: "minio", Name: "acme-5555-data"}, "t"); err != nil {
		t.Fatal(err)
	}
	if err := other.SetState(ctx, "acme", "data", directory.StateMigrating, directory.Transition{To: directory.StateCutover}, "t"); err != nil {
		t.Fatal(err)
	}
	if changed, err := m.dir.Reload(); !changed || err != nil {
		t.Fatalf("reload: %v %v", changed, err)
	}
	m.backendNames = append(m.backendNames, "acme-5555-data")

	r = m.acme(t, "PUT", "/data/"+ekey+"?partNumber=1&uploadId="+url.QueryEscape(uid), []byte("part-one"))
	seen, _ := m.garage.last()
	if r.StatusCode != 200 || seen != "PUT /acme-1111-data/"+ekey+"?partNumber=1&uploadId="+strings.TrimPrefix(uid, gid+"~") {
		t.Fatalf("upload part after the reload: %d, garage saw %q", r.StatusCode, seen)
	}
	r = m.acme(t, "GET", "/data/"+ekey+"?uploadId="+uid, nil)
	if r.StatusCode != 200 || between(r.body, "<UploadId>", "</UploadId>") != uid || !strings.Contains(string(r.body), "<Bucket>data</Bucket>") {
		t.Fatalf("list parts: %d %s", r.StatusCode, r.body)
	}
	r = m.acme(t, "POST", "/data/"+ekey+"?uploadId="+uid, []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>`))
	// Explicit <Location> assertion (docs/STATUS.md): the client-facing host and bucket, never an endpoint.
	if loc := between(r.body, "<Location>", "</Location>"); r.StatusCode != 200 || loc != m.front.URL+"/data/"+strings.ReplaceAll(key, " ", " ") {
		t.Fatalf("complete: %d location %q want %q\n%s", r.StatusCode, loc, m.front.URL+"/data/"+key, r.body)
	}
	if r.ContentLength != int64(len(r.body)) || len(r.TransferEncoding) != 0 {
		t.Fatalf("CompleteMultipartUpload lost its Content-Length (amendment 2): %d / %d %v", r.ContentLength, len(r.body), r.TransferEncoding)
	}
	m.noLeak(t, "CompleteMultipartUpload", r)
	if b, ok := m.garage.object("acme-1111-data", key); !ok || string(b) != "part-one" {
		t.Fatalf("object not completed on the cluster the upload started on: %q %v", b, ok)
	}
}

func TestUploadIDTolerance(t *testing.T) {
	m := newMixedRig(t, nil)
	// An upload started before the codec: the client holds the backend's bare id.
	m.garage.addUpload("legacy-id", "acme-1111-data/k")
	r := m.acme(t, "PUT", "/data/k?partNumber=1&uploadId=legacy-id", []byte("p"))
	if seen := m.garageLast(); r.StatusCode != 200 || seen != "PUT /acme-1111-data/k?partNumber=1&uploadId=legacy-id" {
		t.Fatalf("unprefixed uploadId: %d %q", r.StatusCode, seen)
	}
	before := m.upstreamCalls()
	for _, id := range []string{"abcdef~legacy-id", upstream.ClusterID("minio") + "~legacy-id"} {
		r := m.acme(t, "PUT", "/data/k?partNumber=1&uploadId="+id, []byte("p"))
		if r.StatusCode != 404 || !strings.Contains(string(r.body), "NoSuchUpload") {
			t.Errorf("uploadId %s: %d %s", id, r.StatusCode, r.body)
		}
	}
	if m.upstreamCalls() != before {
		t.Fatal("an uploadId for an unknown cluster, or for a cluster without the bucket, reached a backend")
	}
	// ListMultipartUploads: markers and ids are prefixed, empty markers stay empty.
	r = m.acme(t, "GET", "/data?uploads", nil)
	if r.StatusCode != 200 || !strings.Contains(string(r.body), "<UploadIdMarker></UploadIdMarker>") ||
		!strings.Contains(string(r.body), "<UploadId>"+upstream.ClusterID("garage")+"~legacy-id</UploadId>") {
		t.Fatalf("list uploads: %s", r.body)
	}
}

func TestErrorEchoesRewritten(t *testing.T) {
	m := newMixedRig(t, nil)
	r := m.acme(t, "GET", "/data/no/such%20key", nil)
	if r.StatusCode != 404 || !strings.Contains(string(r.body), "<Resource>/data/no/such key</Resource>") ||
		!strings.Contains(string(r.body), "<BucketName>data</BucketName>") || !strings.Contains(string(r.body), "NoSuchKey for data at "+strings.TrimPrefix(m.front.URL, "http://")) {
		t.Fatalf("path-style error: %d %s", r.StatusCode, r.body)
	}
	if r.ContentLength != int64(len(r.body)) {
		t.Fatalf("error body lost its Content-Length (amendment 2): %d/%d", r.ContentLength, len(r.body))
	}
	m.noLeak(t, "path-style error", r)
	r = m.send(t, "GET", "/no/such%20key", "data.shunt.example.com", nil, acmeAK, acmeSK, nil)
	if r.StatusCode != 404 || !strings.Contains(string(r.body), "<Resource>/no/such key</Resource>") || !strings.Contains(string(r.body), "<BucketName>data</BucketName>") {
		t.Fatalf("virtual-host error: %d %s", r.StatusCode, r.body)
	}
	m.noLeak(t, "virtual-host error", r)
	r = m.send(t, "HEAD", "/nothere", "data.shunt.example.com", nil, acmeAK, acmeSK, nil)
	if r.StatusCode != 404 {
		t.Fatalf("HEAD: %d", r.StatusCode)
	}
	m.noLeak(t, "HEAD error", r)

	// Virtual-host CompleteMultipartUpload: <Location> is the virtual-host URL.
	init := m.send(t, "POST", "/mp?uploads", "data.shunt.example.com", nil, acmeAK, acmeSK, nil)
	uid := between(init.body, "<UploadId>", "</UploadId>")
	m.send(t, "PUT", "/mp?partNumber=1&uploadId="+uid, "data.shunt.example.com", []byte("x"), acmeAK, acmeSK, nil)
	done := m.send(t, "POST", "/mp?uploadId="+uid, "data.shunt.example.com", []byte("<CompleteMultipartUpload/>"), acmeAK, acmeSK, nil)
	if loc := between(done.body, "<Location>", "</Location>"); loc != "http://data.shunt.example.com/mp" {
		t.Fatalf("virtual-host location %q\n%s", loc, done.body)
	}
	m.noLeak(t, "virtual-host CompleteMultipartUpload", done)
}

func TestLargeRewrittenBodySpillsToChunked(t *testing.T) {
	m := newMixedRig(t, nil)
	m.garage.setListPad(2000) // ~120 KiB, past the 64 KiB scratch
	r := m.acme(t, "GET", "/data?list-type=2", nil)
	if r.StatusCode != 200 || r.ContentLength != -1 || len(r.body) < 100<<10 || !strings.Contains(string(r.body[:400]), "<Name>data</Name>") {
		t.Fatalf("large listing: %d len %d cl %d", r.StatusCode, len(r.body), r.ContentLength)
	}
	// The bucket holds 2000 keys; one page returns the S3 maximum of 1000 and says it is truncated.
	if !strings.HasSuffix(string(r.body), "</ListBucketResult>") || strings.Count(string(r.body), "<Contents>") != 1000 {
		t.Fatalf("large listing body incomplete: %d entries", strings.Count(string(r.body), "<Contents>"))
	}
}

func TestShortReadThroughRewriterIsNeverSilent(t *testing.T) {
	for _, cut := range []int{1000, 90 << 10} {
		m := newMixedRig(t, nil)
		m.garage.setListPad(3000)
		m.garage.setCut(cut)
		req, _ := http.NewRequest("GET", m.front.URL+"/data?list-type=2", http.NoBody)
		sigv4.Sign(req, sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, "us-east-1", sigv4.UnsignedPayload, time.Now())
		resp, err := fresh().Do(req)
		if err != nil {
			continue // cut inside the scratch: the connection closed before any response, an error
		}
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err == nil {
			t.Fatalf("cut at %d: truncated listing delivered as complete", cut)
		}
	}
}

func TestCopySourceAndRefusedOps(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/pics/src%20img", []byte("image"))
	r := m.send(t, "PUT", "/data/copy", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Copy-Source": "/pics/src%20img"})
	_, hdr := m.garage.last()
	if r.StatusCode != 200 || hdr.Get("X-Amz-Copy-Source") != "/acme-7777-pics/src%20img" {
		t.Fatalf("same-cluster copy: %d %s, upstream copy source %q", r.StatusCode, r.body, hdr.Get("X-Amz-Copy-Source"))
	}
	if b, _ := m.garage.object("acme-1111-data", "copy"); string(b) != "image" {
		t.Fatalf("copied object: %q", b)
	}
	before := m.upstreamCalls()
	cases := []struct {
		method, target, copySrc string
		status                  int
		code                    string
	}{
		{"PUT", "/data/x", "old/missing", 404, "NoSuchKey"}, // a cross-cluster copy of an object that is not there
		{"PUT", "/data/x", "logs/k", 404, "NoSuchBucket"},   // another tenant's bucket
		{"PUT", "/data/x", "nope/k", 404, "NoSuchBucket"},   // no such bucket
		{"PUT", "/data/x", "justabucket", 400, "InvalidArgument"},
		{"GET", "/data?logging", "", 501, "NotImplemented"},
		{"PUT", "/data?replication", "", 501, "NotImplemented"},
		{"GET", "/data?inventory", "", 501, "NotImplemented"},
		{"GET", "/data?analytics&id=a", "", 501, "NotImplemented"},
		{"GET", "/data?notification", "", 501, "NotImplemented"},
		{"POST", "/?foo", "", 501, "NotImplemented"},
	}
	for _, c := range cases {
		hdr := map[string]string{}
		if c.copySrc != "" {
			hdr["X-Amz-Copy-Source"] = c.copySrc
		}
		r := m.send(t, c.method, c.target, "", nil, acmeAK, acmeSK, hdr)
		if r.StatusCode != c.status || !strings.Contains(string(r.body), c.code) {
			t.Errorf("%s %s (copy %q): %d %s", c.method, c.target, c.copySrc, r.StatusCode, r.body)
		}
	}
	// Only the cross-cluster copy above reaches a backend at all: one HEAD of the source cluster,
	// which answers NoSuchKey (a copy shunt streams is tested in crosscopy_test.go).
	if n := m.upstreamCalls() - before; n != 1 {
		t.Fatalf("refused requests reached a backend %d times", n)
	}
	// Bucket policy passes through unrewritten (amendment 3).
	r = m.acme(t, "GET", "/data?policy", nil)
	if r.StatusCode != 200 || string(r.body) != `{"Version":"2012-10-17","Statement":[]}` || m.garageLast() != "GET /acme-1111-data?policy" {
		t.Fatalf("policy: %d %s %s", r.StatusCode, r.body, m.garageLast())
	}
}

func TestExpectedBucketOwner(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/k", []byte("x"))
	r := m.send(t, "GET", "/data/k", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Expected-Bucket-Owner": ownerID("acme")})
	if _, hdr := m.garage.last(); r.StatusCode != 200 || hdr.Get("X-Amz-Expected-Bucket-Owner") != "" {
		t.Fatalf("matching owner: %d, forwarded %q", r.StatusCode, hdr.Get("X-Amz-Expected-Bucket-Owner"))
	}
	before := m.upstreamCalls()
	if r := m.send(t, "GET", "/data/k", "", nil, acmeAK, acmeSK, map[string]string{"X-Amz-Expected-Bucket-Owner": "123456789012"}); r.StatusCode != 403 {
		t.Fatalf("wrong owner: %d", r.StatusCode)
	}
	if m.upstreamCalls() != before {
		t.Fatal("wrong expected owner reached a backend")
	}
}

func TestSkipOnConnectError(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := l.Addr().String()
	l.Close()
	m := newMixedRig(t, nil, func(c map[string]config.Cluster) {
		g := c["garage"]
		g.Endpoints = append([]string{dead}, g.Endpoints...)
		c["garage"] = g
	})
	for i := 0; i < 4; i++ { // round-robin hits the dead endpoint first on every other request
		if r := m.acme(t, "PUT", fmt.Sprintf("/data/k%d", i), []byte("body")); r.StatusCode != 200 {
			t.Fatalf("request %d with one dead endpoint: %d %s", i, r.StatusCode, r.body)
		}
	}
	if !strings.Contains(m.alerts.String(), "refused the connection") {
		t.Fatalf("no skip logged: %s", m.alerts.String())
	}
}

func TestUpstreamRedirectFailsLoudly(t *testing.T) {
	m := newMixedRig(t, nil)
	m.garage.setRedirect()
	r := m.acme(t, "GET", "/data/k", nil)
	if r.StatusCode != 500 || strings.Contains(string(r.body), "elsewhere") || r.Header.Get("Location") != "" {
		t.Fatalf("redirect: %d %v %s", r.StatusCode, r.Header, r.body)
	}
	if !strings.Contains(m.alerts.String(), "redirect") || !strings.Contains(m.alerts.String(), "eu-west-9") {
		t.Fatalf("redirect not logged loudly: %s", m.alerts.String())
	}
}

func TestMetricLabelsAndAccessLog(t *testing.T) {
	m := newMixedRig(t, nil)
	m.acme(t, "PUT", "/data/k", []byte("x"))
	m.acme(t, "GET", "/old?list-type=2", nil)
	if v := metricCounter(t, m.h.Metrics, "shunt_requests_total", `cluster="garage",cluster_type="s3",op="PutObject",status_class="2xx"`); v != 1 {
		t.Errorf("garage PutObject: %v", v)
	}
	if v := metricCounter(t, m.h.Metrics, "shunt_requests_total", `cluster="minio",cluster_type="minio",op="ListObjectsV2",status_class="2xx"`); v != 1 {
		t.Errorf("minio ListObjectsV2: %v", v)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(m.accessLog.String(), `"op":"ListObjectsV2"`) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	log := m.accessLog.String()
	if !strings.Contains(log, `"cluster":"garage","cluster_type":"s3","tenant":"acme","backend_bucket":"acme-1111-data"`) {
		t.Fatalf("access log: %s", log)
	}
}

func TestRewriteFlagOffLeavesEchoes(t *testing.T) {
	m := newMixedRig(t, nil)
	m.h.Rewrite = false
	r := m.acme(t, "GET", "/data/nokey", nil)
	if !strings.Contains(string(r.body), "acme-1111-data") {
		t.Fatalf("with the rewriter switched off the backend echo is relayed as is; got %s", r.body)
	}
}

// metricCounter reads a counter by its full sorted label string.
func metricCounter(t *testing.T, m *telemetry.Metrics, name, labels string) float64 {
	t.Helper()
	return metric(t, m, name, labels)
}
