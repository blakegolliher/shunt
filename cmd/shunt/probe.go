package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // S3 checksum algorithm, not a security primitive
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/bits"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/sigv4/chunked"
)

// Probe results feed the cluster capability profile (docs/DESIGN.md decision 3) and
// docs/reference/backend-compat.md. POC scope (docs/POC.md): sha256 enforcement, unsigned-trailer
// support, If-None-Match: * on PUT, checksum algorithms honored, unsigned GET / status.
type probeResult struct {
	Endpoint        string            `json:"endpoint"`
	Region          string            `json:"region"`
	UnsignedGetRoot int               `json:"unsigned_get_root_status"`
	EnforcesSHA256  string            `json:"enforces_sha256"`
	UnsignedTrailer string            `json:"unsigned_trailer"`
	Checksums       map[string]string `json:"checksums"`
	IfNoneMatchPut  string            `json:"if_none_match_star_put"`
	IfMatchPut      string            `json:"if_match_put"`
	IfMatchDelete   string            `json:"if_match_delete"`
	Notes           []string          `json:"notes,omitempty"`
}

func newProbe() *cobra.Command {
	var (
		endpoint, region, bucket, caFile, akEnv, skEnv string
		asJSON, insecure                               bool
	)
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "Report what an S3 backend enforces (sha256, unsigned trailers, checksums, conditional PUT)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ak, sk := os.Getenv(akEnv), os.Getenv(skEnv)
			if ak == "" || sk == "" {
				return fmt.Errorf("probe: %s and %s must be set", akEnv, skEnv)
			}
			if insecure {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "WARNING: --insecure: TLS certificate verification is disabled for", endpoint)
			}
			p, err := newProber(endpoint, region, bucket, caFile, ak, sk, insecure)
			if err != nil {
				return err
			}
			res, err := p.run(cmd.Context())
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(res)
			}
			printProbe(cmd.OutOrStdout(), res)
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "backend URL, e.g. https://s3.example:443 (required)")
	cmd.Flags().StringVar(&region, "region", "us-east-1", "signing region")
	cmd.Flags().StringVar(&bucket, "bucket", "", "existing scratch bucket the probe may write to (required)")
	cmd.Flags().StringVar(&caFile, "ca", "", "PEM CA bundle for the endpoint (self-signed backends)")
	cmd.Flags().StringVar(&akEnv, "access-key-env", "AWS_ACCESS_KEY_ID", "env var holding the access key")
	cmd.Flags().StringVar(&skEnv, "secret-env", "AWS_SECRET_ACCESS_KEY", "env var holding the secret")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON instead of a table")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "skip TLS certificate verification (temporary, for backends without a valid certificate)")
	_ = cmd.MarkFlagRequired("endpoint")
	_ = cmd.MarkFlagRequired("bucket")
	return cmd
}

type prober struct {
	base   *url.URL
	region string
	bucket string
	creds  sigv4.Credentials
	client *http.Client
	prefix string
}

func newProber(endpoint, region, bucket, caFile, ak, sk string, insecure bool) (*prober, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("probe: endpoint must be http(s)://host[:port], got %q", endpoint)
	}
	tr := &http.Transport{DisableCompression: true, ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	if caFile != "" {
		pem, err := os.ReadFile(caFile) //nolint:gosec // operator-provided path
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("probe: no certificates in %s", caFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	if insecure {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // explicit operator opt-in, warned on stderr and noted in the result
	}
	var rb [6]byte
	_, _ = rand.Read(rb[:])
	return &prober{base: u, region: region, bucket: bucket, creds: sigv4.Credentials{AccessKey: ak, Secret: sk},
		client: &http.Client{Transport: tr, Timeout: 60 * time.Second}, prefix: "shunt-probe/" + hex.EncodeToString(rb[:]) + "/"}, nil
}

// reply is one probe response.
type reply struct {
	status int
	code   string
	header http.Header
	body   string
}

var codeRe = regexp.MustCompile(`<Code>([^<]*)</Code>`)

// do signs and sends a request; unsigned=true sends it anonymously.
func (p *prober) do(ctx context.Context, method, key string, body []byte, payloadHash string, unsigned bool, hdr map[string]string) (reply, error) {
	u := *p.base
	u.Path = "/" + p.bucket + "/" + key
	if key == "" {
		u.Path = "/"
		if p.bucket != "" && method != http.MethodGet {
			u.Path = "/" + p.bucket
		}
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd) //nolint:gosec // G704: probing the operator-given endpoint is the command's purpose
	if err != nil {
		return reply{}, err
	}
	req.ContentLength = int64(len(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if !unsigned {
		if payloadHash == "" {
			payloadHash = sigv4.UnsignedPayload
		}
		sigv4.Sign(req, p.creds, p.region, payloadHash, time.Now())
	}
	resp, err := p.client.Do(req) //nolint:gosec // G704: probing the operator-given endpoint is the command's purpose
	if err != nil {
		return reply{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // probe
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	r := reply{status: resp.StatusCode, header: resp.Header, body: string(b)}
	if m := codeRe.FindStringSubmatch(r.body); m != nil {
		r.code = m[1]
	}
	return r, nil
}

func (p *prober) put(ctx context.Context, key string, body []byte, payloadHash string, hdr map[string]string) (reply, error) {
	return p.do(ctx, http.MethodPut, key, body, payloadHash, false, hdr)
}

func (p *prober) del(ctx context.Context, key string) {
	_, _ = p.do(ctx, http.MethodDelete, key, nil, "", false, nil)
}

func checksumOf(algo string, data []byte) string {
	var h hash.Hash
	switch algo {
	case "crc32":
		h = crc32.NewIEEE()
	case "crc32c":
		h = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "sha1":
		h = sha1.New() //nolint:gosec // S3 checksum algorithm
	case "sha256":
		h = sha256.New()
	case "crc64nvme":
		h = crc64.New(crc64.MakeTable(bits.Reverse64(0xad93d23594c93659)))
	}
	h.Write(data)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (p *prober) run(ctx context.Context) (*probeResult, error) {
	res := &probeResult{Endpoint: p.base.String(), Region: p.region, Checksums: map[string]string{}}
	if tc := p.client.Transport.(*http.Transport).TLSClientConfig; tc != nil && tc.InsecureSkipVerify { //nolint:errcheck // newProber always builds an *http.Transport
		res.Notes = append(res.Notes, "TLS certificate verification was DISABLED (--insecure)")
	}
	body := []byte("shunt probe payload\n")
	sum := sha256.Sum256(body)
	good := hex.EncodeToString(sum[:])

	// A first signed PUT establishes that credentials, region, and the bucket work at all.
	k0 := p.prefix + "sanity"
	r, err := p.put(ctx, k0, body, good, nil)
	if err != nil {
		return nil, fmt.Errorf("probe: cannot reach %s: %w", p.base, err)
	}
	if r.status == 404 && r.code == "NoSuchBucket" {
		// Scratch bucket missing: create it for the probe and remove it afterwards.
		if cr, cerr := p.do(ctx, http.MethodPut, "", nil, "", false, nil); cerr != nil || cr.status != 200 {
			return nil, fmt.Errorf("probe: bucket %q does not exist and could not be created: %v %d %s", p.bucket, cerr, cr.status, cr.code)
		}
		res.Notes = append(res.Notes, "scratch bucket "+p.bucket+" was created by the probe and deleted afterwards")
		defer func() { _, _ = p.do(ctx, http.MethodDelete, "", nil, "", false, nil) }()
		r, err = p.put(ctx, k0, body, good, nil)
		if err != nil {
			return nil, err
		}
	}
	if r.status != 200 {
		return nil, fmt.Errorf("probe: signed PUT to bucket %q failed: %d %s %s", p.bucket, r.status, r.code, strings.TrimSpace(r.body))
	}
	defer p.del(ctx, k0)

	// 1. Unsigned GET / (health-check behavior, docs/DESIGN.md §2.8).
	if r, err = p.do(ctx, http.MethodGet, "", nil, "", true, nil); err == nil {
		res.UnsignedGetRoot = r.status
	}

	// 2. Hex sha256 mismatch enforcement.
	wrong := hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32))
	k := p.prefix + "sha256"
	r, err = p.put(ctx, k, body, wrong, nil)
	switch {
	case err != nil:
		res.EnforcesSHA256 = "error: " + err.Error()
	case r.status == 400 && r.code == "XAmzContentSHA256Mismatch":
		res.EnforcesSHA256 = "yes"
	case r.status/100 == 4:
		res.EnforcesSHA256 = fmt.Sprintf("yes (%d %s)", r.status, r.code)
	default:
		res.EnforcesSHA256 = fmt.Sprintf("NO (accepted with %d)", r.status)
		p.del(ctx, k)
	}

	// 3. Unsigned trailer: correct crc32 must be accepted, wrong crc32 must be rejected.
	k = p.prefix + "trailer"
	enc := func(b []byte, crc string) []byte {
		t := &fixedTrailer{v: crc}
		e := chunked.NewEncoder(bytes.NewReader(b), int64(len(b)), "x-amz-checksum-crc32", t, chunked.DefaultChunkSize)
		out, _ := io.ReadAll(e)
		return out
	}
	trailerHdr := func(n int) map[string]string {
		return map[string]string{"Content-Encoding": "aws-chunked", "X-Amz-Decoded-Content-Length": fmt.Sprint(n), "X-Amz-Trailer": "x-amz-checksum-crc32"}
	}
	okR, err1 := p.put(ctx, k, enc(body, checksumOf("crc32", body)), sigv4.StreamingUnsignedPayloadTrailer, trailerHdr(len(body)))
	badR, err2 := p.put(ctx, k+"-bad", enc(body, "AAAAAA=="), sigv4.StreamingUnsignedPayloadTrailer, trailerHdr(len(body)))
	switch {
	case err1 != nil || err2 != nil:
		res.UnsignedTrailer = "error"
	case okR.status == 200 && badR.status/100 == 4:
		res.UnsignedTrailer = fmt.Sprintf("yes, checksum validated (%d %s on mismatch)", badR.status, badR.code)
	case okR.status == 200:
		res.UnsignedTrailer = fmt.Sprintf("accepted but checksum NOT validated (mismatch got %d)", badR.status)
	default:
		res.UnsignedTrailer = fmt.Sprintf("NO (%d %s)", okR.status, okR.code)
	}
	p.del(ctx, k)
	p.del(ctx, k+"-bad")

	// 4. Checksum headers per algorithm: correct value accepted, wrong value rejected.
	for _, algo := range []string{"crc32", "crc32c", "sha1", "sha256", "crc64nvme"} {
		hname := "X-Amz-Checksum-" + strings.ToUpper(algo[:1]) + algo[1:]
		kk := p.prefix + "ck-" + algo
		okR, e1 := p.put(ctx, kk, body, good, map[string]string{hname: checksumOf(algo, body)})
		badR, e2 := p.put(ctx, kk+"-bad", body, good, map[string]string{hname: checksumOf(algo, []byte("other"))})
		switch {
		case e1 != nil || e2 != nil:
			res.Checksums[algo] = "error"
		case okR.status == 200 && badR.status/100 == 4:
			res.Checksums[algo] = fmt.Sprintf("validated (%d %s)", badR.status, badR.code)
		case okR.status == 200:
			res.Checksums[algo] = "ignored (wrong value accepted)"
		default:
			res.Checksums[algo] = fmt.Sprintf("rejected (%d %s)", okR.status, okR.code)
		}
		p.del(ctx, kk)
		p.del(ctx, kk+"-bad")
	}

	// 5. Conditional PUT.
	k = p.prefix + "cond"
	if r, err = p.put(ctx, k, body, good, nil); err == nil && r.status == 200 {
		etag := r.header.Get("ETag")
		r2, _ := p.put(ctx, k, body, good, map[string]string{"If-None-Match": "*"})
		switch r2.status {
		case 412:
			res.IfNoneMatchPut = "yes (412)"
		case 200:
			res.IfNoneMatchPut = "NO (overwrote with 200)"
		default:
			res.IfNoneMatchPut = fmt.Sprintf("rejected (%d %s)", r2.status, r2.code)
		}
		r3, _ := p.put(ctx, k, body, good, map[string]string{"If-Match": `"0000000000000000000000000000dead"`})
		r4, _ := p.put(ctx, k, body, good, map[string]string{"If-Match": etag})
		switch {
		case r3.status == 412 && r4.status == 200:
			res.IfMatchPut = "yes (412 on mismatch, 200 on match)"
		case r3.status == 200:
			res.IfMatchPut = "NO (mismatch accepted)"
		default:
			res.IfMatchPut = fmt.Sprintf("rejected (%d %s / %d %s)", r3.status, r3.code, r4.status, r4.code)
		}
		// 6. Conditional DELETE: a mismatched If-Match must leave the object, a matching one remove it.
		// The mover's withdrawal of its own copy depends on it (capabilities.conditional_delete).
		etag = r4.header.Get("ETag")
		if etag == "" {
			etag = r.header.Get("ETag")
		}
		d1, _ := p.do(ctx, http.MethodDelete, k, nil, "", false, map[string]string{"If-Match": `"0000000000000000000000000000dead"`})
		h1, _ := p.do(ctx, http.MethodHead, k, nil, "", false, nil)
		switch {
		case h1.status == 404:
			res.IfMatchDelete = fmt.Sprintf("NO (mismatch deleted the object, %d)", d1.status)
		case d1.status == 412:
			d2, _ := p.do(ctx, http.MethodDelete, k, nil, "", false, map[string]string{"If-Match": etag})
			h2, _ := p.do(ctx, http.MethodHead, k, nil, "", false, nil)
			if d2.status/100 == 2 && h2.status == 404 {
				res.IfMatchDelete = fmt.Sprintf("yes (412 on mismatch, %d on match)", d2.status)
			} else {
				res.IfMatchDelete = fmt.Sprintf("mismatch refused (412) but match did not delete (%d %s, then HEAD %d)", d2.status, d2.code, h2.status)
			}
		default:
			res.IfMatchDelete = fmt.Sprintf("rejected (%d %s), object kept", d1.status, d1.code)
		}
		p.del(ctx, k)
	}
	return res, nil
}

type fixedTrailer struct{ v string }

func (f *fixedTrailer) Algorithm() string { return "CRC32" }
func (f *fixedTrailer) Checksum() string  { return f.v }

func printProbe(w io.Writer, r *probeResult) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(k, v string) { _, _ = fmt.Fprintf(tw, "%s\t%s\n", k, v) }
	row("endpoint", r.Endpoint+" (region "+r.Region+")")
	row("unsigned GET /", fmt.Sprint(r.UnsignedGetRoot))
	row("enforces x-amz-content-sha256", r.EnforcesSHA256)
	row("STREAMING-UNSIGNED-PAYLOAD-TRAILER", r.UnsignedTrailer)
	for _, a := range []string{"crc32", "crc32c", "sha1", "sha256", "crc64nvme"} {
		row("x-amz-checksum-"+a, r.Checksums[a])
	}
	row("If-None-Match: * on PUT", r.IfNoneMatchPut)
	row("If-Match on PUT", r.IfMatchPut)
	row("If-Match on DELETE", r.IfMatchDelete)
	for _, n := range r.Notes {
		row("note", n)
	}
	_ = tw.Flush()
}
