// Command s3diff runs an S3 operation matrix against a backend directly and through shunt and
// diffs status, headers (minus an allowlist), and body (docs/DESIGN.md §6). The proxy's core
// property is transparency; this measures it. Test tool only: it imports aws-sdk-go-v2
// (Apache-2.0, docs/deps.md) and never links into the proxy binary.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// headerAllow lists headers that legitimately differ between direct and proxied responses.
var headerAllow = map[string]bool{
	"Date": true, "Server": true, "X-Amz-Request-Id": true, "X-Amz-Id-2": true,
	"Via": true, "X-Shunt-Request-Id": true, "Connection": true, "Keep-Alive": true,
	"X-Amz-Version-Id": true, // each side does its own PUT; versioned backends mint a new id per write
	"X-Vast-Rcf-Id":    true, // VAST's per-request trace id, the equivalent of x-amz-request-id
}

// xmlNoise strips per-request values from XML bodies before comparing them.
var xmlNoise = regexp.MustCompile(`<(UploadId|RequestId|HostId|LastModified|CreationDate|Initiated|Date|VersionId|DeleteMarkerVersionId)>[^<]*</`)

// locationPort strips the port from <Location>: backends echo the Host header into it, and
// shunt's listener port differs from the backend's. Passthrough preserves Host by design.
var locationPort = regexp.MustCompile(`(<Location>[a-z]+://[^:/<]+):\d+`)

// probe is what we compare: the last HTTP response seen by a client.
type probe struct {
	status  int
	headers http.Header
	bodyLen int64
	bodySum string
	body    []byte // kept for XML normalisation only (small responses)
	err     string
}

// recorder captures every response on a transport since the last take(). Bodies are hashed as
// the SDK reads them, so a multi-step case (multipart) compares each step in order.
type recorder struct {
	rt   http.RoundTripper
	mu   sync.Mutex
	seen []*probe
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.rt.RoundTrip(req)
	p := &probe{}
	if err != nil {
		p.err = err.Error()
		r.set(p)
		return resp, err
	}
	p.status = resp.StatusCode
	p.headers = resp.Header.Clone()
	hb := &hashBody{ReadCloser: resp.Body, h: sha256.New(), p: p}
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "xml") || ct == "" {
		hb.keep = &bytes.Buffer{} // XML, or unlabelled (Garage sends XML with no Content-Type)
	}
	resp.Body = hb
	r.set(p)
	return resp, nil
}

func (r *recorder) set(p *probe) { r.mu.Lock(); r.seen = append(r.seen, p); r.mu.Unlock() }
func (r *recorder) take() []*probe {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.seen
	r.seen = nil
	if len(out) == 0 {
		out = []*probe{{err: "no response recorded"}}
	}
	return out
}

type hashBody struct {
	io.ReadCloser
	h    hash.Hash
	p    *probe
	keep *bytes.Buffer
}

func (b *hashBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.h.Write(p[:n])
		b.p.bodyLen += int64(n)
		if b.keep != nil && b.keep.Len() < 1<<20 {
			b.keep.Write(p[:n])
		}
	}
	if err == io.EOF {
		b.p.bodySum = hex.EncodeToString(b.h.Sum(nil))
		if b.keep != nil {
			b.p.body = b.keep.Bytes()
		}
	}
	return n, err
}

// target is one side of the comparison: an SDK client and a raw client sharing one recorder.
type target struct {
	name     string
	client   *s3.Client
	rec      *recorder
	httpc    *http.Client
	endpoint string
	ak, sk   string
	region   string
}

// newTarget builds the clients. When addr is set, every *.domain / domain host dials addr.
func newTarget(name, endpoint, addr, domain, region, ak, sk, caFile string, pathStyle, insecure bool) (*target, error) {
	tr := &http.Transport{
		DisableCompression: true,
		ForceAttemptHTTP2:  false,
		TLSNextProto:       map[string]func(string, *tls.Conn) http.RoundTripper{},
		DialContext: func(ctx context.Context, network, host string) (net.Conn, error) {
			h, _, err := net.SplitHostPort(host)
			if addr != "" && err == nil && (h == domain || strings.HasSuffix(h, "."+domain)) {
				host = addr
			}
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, host)
		},
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	if insecure {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // explicit -direct-insecure opt-in, temporary
	}
	rec := &recorder{rt: tr}
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(ak, sk, ""),
		HTTPClient:  &http.Client{Transport: rec},
		// POC-1 compares one signing mode; aws-chunked trailers are POC-2's dimension.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           1,
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = pathStyle
	})
	return &target{name: name, client: c, rec: rec, httpc: &http.Client{Transport: rec}, endpoint: endpoint, ak: ak, sk: sk, region: region}, nil
}

type result struct {
	name   string
	op     string
	direct []*probe
	via    []*probe
	diffs  []string
	gap    bool // the backend rejected the direct request that shunt accepted: a backend gap, not a shunt diff
	// statusOnly compares the status and nothing else: from POC-3 shunt answers some requests from
	// the directory instead of forwarding them (ListBuckets, a bucket it does not know), so the
	// backend's headers and body are legitimately absent.
	statusOnly bool
}

func (r *result) compare(compareBody bool) {
	if len(r.direct) != len(r.via) {
		r.diffs = append(r.diffs, fmt.Sprintf("step count: direct=%d via=%d", len(r.direct), len(r.via)))
		return
	}
	for i := range r.direct {
		r.compareStep(i, r.direct[i], r.via[i], compareBody)
	}
}

func (r *result) compareStep(i int, d, v *probe, compareBody bool) {
	prefix := ""
	if len(r.direct) > 1 {
		prefix = fmt.Sprintf("step %d: ", i+1)
	}
	add := func(format string, a ...any) { r.diffs = append(r.diffs, prefix+fmt.Sprintf(format, a...)) }
	if d.err != "" || v.err != "" {
		if d.err != v.err {
			add("error: direct=%q via=%q", d.err, v.err)
		}
		return
	}
	if r.statusOnly {
		if d.status != v.status {
			add("status: direct=%d via=%d", d.status, v.status)
		}
		return
	}
	if d.status != v.status {
		add("status: direct=%d via=%d", d.status, v.status)
		if len(d.body) > 0 {
			add("direct body: %s", trunc(string(d.body)))
		}
		if len(v.body) > 0 {
			add("via body: %s", trunc(string(v.body)))
		}
	}
	keys := map[string]bool{}
	for k := range d.headers {
		keys[k] = true
	}
	for k := range v.headers {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		if !headerAllow[k] {
			sorted = append(sorted, k)
		}
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if k == "Content-Length" && (d.body != nil || v.body != nil) {
			continue // XML bodies are compared after normalisation; their length follows
		}
		if k == "Content-Length" && d.status == http.StatusNoContent && v.status == http.StatusNoContent {
			continue // RFC 9110 §8.6: a 204 must not carry Content-Length; Go's server drops a backend's "0"
		}
		if a, b := mixedNames(strings.Join(d.headers[k], ",")), mixedNames(strings.Join(v.headers[k], ",")); a != b {
			add("header %s: direct=%q via=%q", k, a, b)
		}
	}
	if !compareBody {
		return
	}
	if d.body != nil || v.body != nil {
		a, b := normalise(d.body), normalise(v.body)
		if a != b {
			add("xml body differs (normalised):\n  direct: %s\n  via:    %s", trunc(a), trunc(b))
		}
		return
	}
	if d.bodyLen != v.bodyLen || d.bodySum != v.bodySum {
		add("body: direct=%d/%s via=%d/%s", d.bodyLen, short(d.bodySum), v.bodyLen, short(v.bodySum))
	}
}

// owned reports whether a CreateBucket failed only because the bucket is already ours.
func owned(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "BucketAlreadyOwnedByYou") || strings.Contains(msg, "BucketAlreadyExists")
}

// parseSizes reads the -sizes list.
func parseSizes(list string) ([]int, error) {
	parts := strings.Split(list, ",")
	sizes := make([]int, 0, len(parts))
	for _, s := range parts {
		var n int
		if _, err := fmt.Sscan(s, &n); err != nil {
			return nil, fmt.Errorf("bad size %q: %w", s, err)
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}

// rewritesLocation is set in resign and mixed modes, where shunt replaces <Location> with the
// client-facing URL (ADR-0006) while each backend builds its own: Garage from its root_domain,
// MinIO from the request Host. Its contents are asserted directly instead of being compared.
var rewritesLocation bool

// mixedLocation blanks <Location> in those modes: the two sides address different clusters, and
// backends build it differently (Garage from its root_domain, MinIO from the request Host), so the
// value is asserted explicitly instead (leakCheck) rather than compared.
var mixedLocation = regexp.MustCompile(`<Location>[^<]*</Location>`)

func normalise(body []byte) string {
	out := mixedNames(string(body))
	if rewritesLocation {
		out = mixedLocation.ReplaceAllString(out, "<Location>*</Location>")
	}
	out = xmlNoise.ReplaceAllString(out, "<$1>*</")
	out = locationPort.ReplaceAllString(out, "$1")
	return sortBuckets(out)
}

// mixedNames maps backend bucket names and cluster endpoints to what a client sees. It is a no-op
// outside -mixed mode.
func mixedNames(s string) string {
	if mixedNormalise == nil {
		return s
	}
	return mixedNormalise.Replace(s)
}

// bucketEntry matches one <Bucket> of a ListBuckets response. Backends are not required to return
// buckets in a stable order (Garage does not), so the comparison sorts them; shunt forwards the
// backend's bytes unchanged either way.
var bucketEntry = regexp.MustCompile(`<Bucket>.*?</Bucket>`)

func sortBuckets(body string) string {
	i, j := strings.Index(body, "<Buckets>"), strings.Index(body, "</Buckets>")
	if i < 0 || j < i {
		return body
	}
	inner := body[i+len("<Buckets>") : j]
	entries := bucketEntry.FindAllString(inner, -1)
	sort.Strings(entries)
	return body[:i+len("<Buckets>")] + strings.Join(entries, "") + body[j:]
}

// truncLen caps the diff detail printed per body; S3DIFF_TRUNC raises it while investigating one.
var truncLen = func() int {
	if v := os.Getenv("S3DIFF_TRUNC"); v != "" {
		var n int
		if _, err := fmt.Sscan(v, &n); err == nil && n > 0 {
			return n
		}
	}
	return 300
}()

func trunc(s string) string {
	if len(s) > truncLen {
		return s[:truncLen] + "…"
	}
	return s
}
func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

var trickyKeys = map[string]string{
	"plain":    "dir/plain.bin",
	"spaces":   "dir with space/file name.bin",
	"unicode":  "dir/ünïcödé-日本語.bin",
	"plus":     "dir/a+b.bin",
	"percent":  "dir/100%.bin",
	"dslash":   "dir//double.bin",
	"trailing": "dir/trailing/",
	"long":     strings.Repeat("k", 1024),
}

func main() {
	var (
		directEP  = flag.String("direct", "http://shunt.example.com", "backend endpoint URL (host is rewritten to -direct-addr)")
		directAd  = flag.String("direct-addr", "127.0.0.1:3900", "backend host:port")
		viaEP     = flag.String("via", "https://shunt.example.com:8443", "shunt endpoint URL")
		viaAd     = flag.String("via-addr", "127.0.0.1:8443", "shunt host:port")
		domain    = flag.String("domain", "shunt.example.com", "wildcard base domain")
		region    = flag.String("region", "garage", "signing region")
		caFile    = flag.String("ca", "test/e2e/certs/wildcard.crt", "CA for the shunt endpoint")
		bucket    = flag.String("bucket", "s3diff", "bucket name (created and deleted)")
		sizesFlag = flag.String("sizes", "0,1,4096,1048576,67108864", "object sizes")
		keep      = flag.Bool("keep", false, "keep the bucket afterwards")
		mode      = flag.String("mode", "passthrough", "passthrough|resign; resign signs the via side with shunt-issued credentials")
		dAKEnv    = flag.String("direct-access-key-env", "AWS_ACCESS_KEY_ID", "env var with the backend access key")
		dSKEnv    = flag.String("direct-secret-env", "AWS_SECRET_ACCESS_KEY", "env var with the backend secret")
		vAKEnv    = flag.String("via-access-key-env", "", "env var with the via-side access key (default SHUNT_ACCESS_KEY in resign mode, else the direct one)")
		vSKEnv    = flag.String("via-secret-env", "", "env var with the via-side secret (default SHUNT_SECRET in resign mode, else the direct one)")
		viaRegion = flag.String("via-region", "", "via-side signing region (default us-east-1 in resign mode, else -region)")
		dInsecure = flag.Bool("direct-insecure", false, "skip TLS verification on the direct side (temporary, for backends without a valid certificate)")
		stylesArg = flag.String("styles", "path,vhost", "addressing styles to run")
		existing  = flag.String("existing-bucket", "", "use this existing bucket; nothing is created or deleted except keys under a unique prefix")
		sigModes  = flag.Bool("signing-modes", true, "run the signing-mode dimension (raw client, every payload mode)")
		mixed     = flag.Bool("mixed", false, "POC-3 mixed-backend mode: compare every bucket against the cluster that holds it")
		cfgPath   = flag.String("config", "test/e2e/data/shunt-mixed.yaml", "with -mixed: the shunt config naming the clusters, directory, and credentials")
	)
	flag.Parse()
	rewritesLocation = *mixed || *mode == "resign"
	if *mixed {
		sizes, err := parseSizes(*sizesFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(runMixed(context.Background(), *cfgPath, *viaEP, *viaAd, *domain, *caFile, sizes))
	}
	if *vAKEnv == "" {
		*vAKEnv, *vSKEnv = *dAKEnv, *dSKEnv
		if *mode == "resign" {
			*vAKEnv, *vSKEnv = "SHUNT_ACCESS_KEY", "SHUNT_SECRET"
		}
	}
	if *viaRegion == "" {
		*viaRegion = *region
		if *mode == "resign" {
			*viaRegion = "us-east-1" // client scope region differs from the cluster region on purpose (ADR-0001)
		}
	}
	ak, sk := os.Getenv(*dAKEnv), os.Getenv(*dSKEnv)
	vak, vsk := os.Getenv(*vAKEnv), os.Getenv(*vSKEnv)
	if ak == "" || sk == "" || vak == "" || vsk == "" {
		fmt.Fprintf(os.Stderr, "credentials required: %s, %s, %s, %s\n", *dAKEnv, *dSKEnv, *vAKEnv, *vSKEnv)
		os.Exit(2)
	}
	if *dInsecure {
		fmt.Fprintln(os.Stderr, "WARNING: -direct-insecure: TLS certificate verification is disabled for", *directEP)
	}
	var runID [4]byte
	_, _ = rand.Read(runID[:])
	sizes, err := parseSizes(*sizesFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx := context.Background()
	var results []result
	failed, gaps := 0, 0
	for _, style := range strings.Split(*stylesArg, ",") {
		pathStyle := style == "path"
		direct, err := newTarget("direct", *directEP, *directAd, *domain, *region, ak, sk, "", pathStyle, *dInsecure)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		via, err := newTarget("via", *viaEP, *viaAd, *domain, *viaRegion, vak, vsk, *caFile, pathStyle, false)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		bkt := *bucket + "-" + style
		prefix := ""
		if *existing != "" {
			bkt = *existing
			prefix = "s3diff/" + hex.EncodeToString(runID[:]) + "/" + style + "/"
		}
		var runMode func(name, op string, body, statusOnly bool, f func(*target) error)
		run := func(name, op string, body bool, f func(*target) error) { runMode(name, op, body, false, f) }
		runMode = func(name, op string, body, statusOnly bool, f func(*target) error) {
			r := result{name: style + "/" + name, op: op, statusOnly: statusOnly}
			ed := f(direct)
			r.direct = direct.rec.take()
			ev := f(via)
			r.via = via.rec.take()
			r.compare(body)
			if errors.Is(ed, errContent) {
				r.diffs = append(r.diffs, "direct: "+errContent.Error())
			}
			if errors.Is(ev, errContent) {
				r.diffs = append(r.diffs, "via: "+errContent.Error())
			}
			if strings.HasPrefix(name, "sig/") && op == "PutObject" && len(r.diffs) > 0 &&
				r.direct[len(r.direct)-1].status >= 400 && r.via[len(r.via)-1].status/100 == 2 {
				r.gap = true
				gaps++
			} else if len(r.diffs) > 0 {
				failed++
			}
			results = append(results, r)
		}

		// Bucket lifecycle: create direct, list via both, delete at the end via both (second is 404).
		if *existing == "" {
			// A bucket left by an interrupted run is reused: its keys are the same on both sides,
			// so they cannot produce a diff, and the run cleans up what it writes.
			if _, err := direct.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bkt}); err != nil && !owned(err) {
				fmt.Fprintf(os.Stderr, "create bucket %s: %v\n", bkt, err)
				os.Exit(2)
			}
			direct.rec.take() // CreateBucket above is setup, not a compared step
		}
		run("bucket", "HeadBucket", false, func(t *target) error {
			_, err := t.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bkt})
			return err
		})
		// In resign mode shunt answers ListBuckets from the directory, so its body is the tenant's
		// bucket set, not the backend's account: only the status is comparable. Mixed mode asserts
		// the contents against the directory instead.
		runMode("bucket", "ListBuckets", *mode != "resign", *mode == "resign", func(t *target) error {
			_, err := t.client.ListBuckets(ctx, &s3.ListBucketsInput{})
			return err
		})

		for kname, key := range trickyKeys {
			if kname == "long" {
				key = strings.Repeat("k", 1024-len(prefix)-len("-1048576"))
			}
			key = prefix + key
			for _, size := range sizes {
				if kname != "plain" && size > 4096 {
					continue // tricky keys are about naming; sizes are about streaming
				}
				payload := deterministic(size)
				sum := sha256.Sum256(payload)
				k := key
				if size > 0 || kname == "plain" {
					k = fmt.Sprintf("%s-%d", key, size)
					if kname == "trailing" {
						k = key + fmt.Sprint(size) + "/"
					}
				}
				label := fmt.Sprintf("%s/%d", kname, size)
				run(label, "PutObject", false, func(t *target) error {
					_, err := t.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bkt, Key: &k, Body: bytes.NewReader(payload), ContentLength: aws.Int64(int64(size))})
					return err
				})
				run(label, "HeadObject", false, func(t *target) error {
					_, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bkt, Key: &k})
					return err
				})
				run(label, "GetObject", true, func(t *target) error {
					out, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bkt, Key: &k})
					if err != nil {
						return err
					}
					got, _ := io.ReadAll(out.Body)
					_ = out.Body.Close()
					if sha256.Sum256(got) != sum {
						return errContent
					}
					return nil
				})
				if size > 100 {
					run(label, "GetObject(range)", true, func(t *target) error {
						out, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bkt, Key: &k, Range: aws.String("bytes=1-100")})
						if err != nil {
							return err
						}
						_, _ = io.Copy(io.Discard, out.Body)
						return out.Body.Close()
					})
				}
			}
		}
		// The signing-mode dimension runs once, path-style, after the SDK matrix's PUTs.
		if *sigModes && pathStyle {
			for _, m := range signingModes() {
				for _, size := range []int{1, 200 << 10} {
					payload := deterministic(size)
					sum := sha256.Sum256(payload)
					k := prefix + fmt.Sprintf("sig/%s-%d", m.name, size)
					label := fmt.Sprintf("sig/%s/%d", m.name, size)
					run(label, "PutObject", false, func(t *target) error { return t.rawPut(ctx, bkt, k, payload, m) })
					if m.payload == "presigned" {
						run(label, "GetObject(presigned)", true, func(t *target) error { return t.rawPresignedGet(ctx, bkt, k, sum) })
						continue
					}
					run(label, "GetObject", true, func(t *target) error {
						out, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bkt, Key: &k})
						if err != nil {
							return err
						}
						got, _ := io.ReadAll(out.Body)
						_ = out.Body.Close()
						if sha256.Sum256(got) != sum {
							return errContent
						}
						return nil
					})
				}
			}
		}

		run("list", "ListObjectsV2", true, func(t *target) error {
			_, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bkt, Prefix: aws.String(prefix + "dir")})
			return err
		})
		run("list", "ListObjectsV2(delim)", true, func(t *target) error {
			_, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bkt, Prefix: aws.String(prefix), Delimiter: aws.String("/"), MaxKeys: aws.Int32(3)})
			return err
		})
		run("list", "ListObjects", true, func(t *target) error {
			_, err := t.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: &bkt, Prefix: aws.String(prefix + "dir/")})
			return err
		})

		// Multipart: 2 parts of 5 MiB + 1 MiB, same key sequentially on both sides.
		mpKey := prefix + "dir/multipart.bin"
		part1, part2 := deterministic(5<<20), deterministic(1<<20)
		run("multipart", "CreateMultipartUpload", true, func(t *target) error {
			out, err := t.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bkt, Key: &mpKey})
			if err != nil {
				return err
			}
			uid := out.UploadId
			p1, err := t.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bkt, Key: &mpKey, UploadId: uid, PartNumber: aws.Int32(1), Body: bytes.NewReader(part1), ContentLength: aws.Int64(int64(len(part1)))})
			if err != nil {
				return err
			}
			p2, err := t.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bkt, Key: &mpKey, UploadId: uid, PartNumber: aws.Int32(2), Body: bytes.NewReader(part2), ContentLength: aws.Int64(int64(len(part2)))})
			if err != nil {
				return err
			}
			_, err = t.client.ListParts(ctx, &s3.ListPartsInput{Bucket: &bkt, Key: &mpKey, UploadId: uid})
			if err != nil {
				return err
			}
			_, err = t.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &bkt, Key: &mpKey, UploadId: uid,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: p1.ETag, PartNumber: aws.Int32(1)}, {ETag: p2.ETag, PartNumber: aws.Int32(2)}}}})
			return err
		})
		run("multipart", "HeadObject", false, func(t *target) error {
			_, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bkt, Key: &mpKey})
			return err
		})
		run("multipart", "AbortMultipartUpload", false, func(t *target) error {
			out, err := t.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bkt, Key: aws.String(prefix + "dir/aborted.bin")})
			if err != nil {
				return err
			}
			_, err = t.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bkt, Key: aws.String(prefix + "dir/aborted.bin"), UploadId: out.UploadId})
			return err
		})

		// Errors: the proxy must relay them unchanged.
		run("errors", "GetObject(404)", true, func(t *target) error {
			_, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bkt, Key: aws.String(prefix + "does/not/exist")})
			return err
		})
		// A bucket the directory does not name never reaches a backend in resign mode: shunt answers
		// it with its own error body and none of the backend's headers.
		runMode("errors", "HeadBucket(404)", false, *mode == "resign", func(t *target) error {
			_, err := t.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bkt + "-nope")})
			return err
		})

		// Deletes: put direct, delete via each side in turn (the second sees the object again).
		delKey := prefix + "dir/todelete.bin"
		run("delete", "DeleteObject", false, func(t *target) error {
			if _, err := direct.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bkt, Key: &delKey, Body: bytes.NewReader([]byte("x"))}); err != nil {
				return err
			}
			direct.rec.take() // setup
			_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bkt, Key: &delKey})
			return err
		})
		run("delete", "DeleteObjects", true, func(t *target) error {
			var ids []types.ObjectIdentifier
			for i := 0; i < 3; i++ {
				k := fmt.Sprintf("%sdir/batch-%d", prefix, i)
				if _, err := direct.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bkt, Key: &k, Body: bytes.NewReader([]byte("x"))}); err != nil {
					return err
				}
				ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
			}
			direct.rec.take() // setup
			_, err := t.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &bkt, Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)}})
			return err
		})

		if !*keep {
			// Remove everything this run wrote (only the run's prefix in an existing bucket).
			var token *string
			for {
				list, err := direct.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bkt, Prefix: aws.String(prefix), ContinuationToken: token})
				if err != nil {
					break
				}
				for _, o := range list.Contents {
					_, _ = direct.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bkt, Key: o.Key})
				}
				if list.IsTruncated == nil || !*list.IsTruncated {
					break
				}
				token = list.NextContinuationToken
			}
			direct.rec.take()
		}
		if !*keep && *existing == "" {
			run("bucket", "DeleteBucket", false, func(t *target) error {
				if _, err := direct.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bkt}); err != nil {
					if _, err := direct.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bkt}); err != nil {
						return err
					}
				}
				direct.rec.take() // setup traffic is not part of the comparison
				_, err := t.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bkt})
				return err
			})
		}
	}

	// Report.
	fmt.Printf("%-44s %-22s %-9s %-9s %s\n", "case", "op", "direct", "via", "result")
	type tally struct{ cases, ok, diff, gap int }
	groups := map[string]*tally{}
	var order []string
	for _, r := range results {
		res := "ok"
		switch {
		case r.gap:
			res = "GAP(backend)"
		case len(r.diffs) > 0:
			res = "DIFF"
		}
		fmt.Printf("%-44s %-22s %-9s %-9s %s\n", r.name, r.op, st(r.direct), st(r.via), res)
		for _, d := range r.diffs {
			fmt.Printf("    %s\n", d)
		}
		g := "sdk matrix (" + strings.SplitN(r.name, "/", 2)[0] + ")"
		if parts := strings.Split(r.name, "/"); len(parts) > 2 && parts[1] == "sig" {
			g = "signing mode " + parts[2]
		}
		if groups[g] == nil {
			groups[g] = &tally{}
			order = append(order, g)
		}
		t := groups[g]
		t.cases++
		switch {
		case r.gap:
			t.gap++
		case len(r.diffs) > 0:
			t.diff++
		default:
			t.ok++
		}
	}
	fmt.Printf("\nmode=%s direct=%s via=%s (via region %s)\n", *mode, *directEP, *viaEP, *viaRegion)
	fmt.Printf("%-48s %6s %6s %6s %6s\n", "group", "cases", "ok", "diff", "gap")
	for _, g := range order {
		t := groups[g]
		fmt.Printf("%-48s %6d %6d %6d %6d\n", g, t.cases, t.ok, t.diff, t.gap)
	}
	fmt.Printf("\n%d cases, %d diffs, %d backend gaps\n", len(results), failed, gaps)
	if failed > 0 {
		os.Exit(1)
	}
}

func st(ps []*probe) string {
	p := ps[len(ps)-1]
	if p.err != "" && p.status == 0 {
		return "ERR"
	}
	if len(ps) > 1 {
		return fmt.Sprintf("%d(%d)", p.status, len(ps))
	}
	return fmt.Sprint(p.status)
}

// deterministic returns n bytes that differ across positions so truncation and reordering show.
func deterministic(n int) []byte {
	b := make([]byte, n)
	var x uint32 = 2463534242
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x) //nolint:gosec // G115: the low byte is the intent
	}
	return b
}
