package main

// The signing-mode dimension (POC-2, docs/DESIGN.md P2 item 7): every client payload mode, sent by
// a raw client built on shunt's own signer and chunk encoders, because aws-sdk-go-v2 cannot emit
// the signed-chunk modes at all. The backend is the oracle for the client encoding: a direct
// request the backend accepts proves the wire form is right, and the via request must then match.

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // S3 checksum algorithm, not a security primitive
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/bits"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/sigv4/chunked"
)

// errContent marks a GET whose body does not match what the case uploaded.
var errContent = errors.New("body does not match the uploaded payload")

type sigMode struct {
	name    string
	payload string // x-amz-content-sha256 value, "sha256" for a hex digest, "presigned"
	algo    string // trailer checksum algorithm, "" for none
}

var checksumAlgos = []string{"crc32", "crc32c", "sha1", "sha256", "crc64nvme"}

func signingModes() []sigMode {
	m := []sigMode{
		{"unsigned", sigv4.UnsignedPayload, ""},
		{"sha256", "sha256", ""},
		{"streaming-signed", sigv4.StreamingSignedPayload, ""},
	}
	for _, a := range checksumAlgos {
		m = append(m, sigMode{"streaming-signed-trailer-" + a, sigv4.StreamingSignedPayloadTrailer, a})
	}
	for _, a := range checksumAlgos {
		m = append(m, sigMode{"unsigned-trailer-" + a, sigv4.StreamingUnsignedPayloadTrailer, a})
	}
	return append(m, sigMode{"presigned", "presigned", ""})
}

func newChecksum(algo string) hash.Hash {
	switch algo {
	case "crc32":
		return crc32.NewIEEE()
	case "crc32c":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "sha1":
		return sha1.New() //nolint:gosec // S3 checksum algorithm
	case "sha256":
		return sha256.New()
	case "crc64nvme":
		return crc64.New(crc64.MakeTable(bits.Reverse64(0xad93d23594c93659)))
	}
	return nil
}

// sumTrailer reports the checksum of the bytes that passed through its hash.
type sumTrailer struct {
	h    hash.Hash
	algo string
}

func (s *sumTrailer) Algorithm() string { return strings.ToUpper(s.algo) }
func (s *sumTrailer) Checksum() string  { return base64.StdEncoding.EncodeToString(s.h.Sum(nil)) }

// escapeKey percent-encodes each path segment the way S3 clients do, keeping "/".
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = sigv4.Escape(parts[i])
	}
	return strings.Join(parts, "/")
}

func (t *target) objectURL(bucket, key string) string {
	return strings.TrimRight(t.endpoint, "/") + "/" + bucket + "/" + escapeKey(key)
}

func (t *target) send(req *http.Request) error {
	resp, err := t.httpc.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// rawPut uploads payload to bucket/key in signing mode m.
func (t *target) rawPut(ctx context.Context, bucket, key string, payload []byte, m sigMode) error {
	now := time.Now().UTC()
	u := t.objectURL(bucket, key)
	creds := sigv4.Credentials{AccessKey: t.ak, Secret: t.sk}
	n := int64(len(payload))
	switch m.payload {
	case "presigned":
		signed := t.presign(http.MethodPut, u, now, 300)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, signed, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.ContentLength = n
		return t.send(req)

	case sigv4.UnsignedPayload, "sha256":
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.ContentLength = n
		hashValue := sigv4.UnsignedPayload
		if m.payload == "sha256" {
			sum := sha256.Sum256(payload)
			hashValue = hex.EncodeToString(sum[:])
		}
		sigv4.Sign(req, creds, t.region, hashValue, now)
		return t.send(req)

	case sigv4.StreamingUnsignedPayloadTrailer:
		name := "x-amz-checksum-" + m.algo
		h := newChecksum(m.algo)
		enc := chunked.NewEncoder(io.TeeReader(bytes.NewReader(payload), h), n, name, &sumTrailer{h: h, algo: m.algo}, chunked.DefaultChunkSize)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, enc)
		if err != nil {
			return err
		}
		req.ContentLength = chunked.EncodedLength(n, chunked.DefaultChunkSize, name, chunked.Base64Len(strings.ToUpper(m.algo)))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(n, 10))
		req.Header.Set("X-Amz-Trailer", name)
		sigv4.Sign(req, creds, t.region, sigv4.StreamingUnsignedPayloadTrailer, now)
		return t.send(req)

	case sigv4.StreamingSignedPayload, sigv4.StreamingSignedPayloadTrailer:
		name, checksumLen := "", 0
		var h hash.Hash
		if m.algo != "" {
			name = "x-amz-checksum-" + m.algo
			h = newChecksum(m.algo)
			checksumLen = chunked.Base64Len(strings.ToUpper(m.algo))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, http.NoBody)
		if err != nil {
			return err
		}
		req.ContentLength = chunked.SignedEncodedLength(n, chunked.DefaultChunkSize, name, checksumLen)
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(n, 10))
		if name != "" {
			req.Header.Set("X-Amz-Trailer", name)
		}
		sigv4.Sign(req, creds, t.region, m.payload, now) // the seed signature
		auth := req.Header.Get("Authorization")
		seed := auth[strings.LastIndex(auth, "Signature=")+len("Signature="):]
		key := sigv4.SigningKey(t.sk, sigv4.Scope{Date: now.Format(sigv4.DateFormat), Region: t.region, Service: sigv4.Service})
		req.Body = io.NopCloser(chunked.NewSignedEncoder(bytes.NewReader(payload), key, seed, now, t.region, name, h, chunked.DefaultChunkSize))
		return t.send(req)
	}
	return errors.New("unknown signing mode " + m.name)
}

// rawPresignedGet reads bucket/key through a presigned URL and checks the body.
func (t *target) rawPresignedGet(ctx context.Context, bucket, key string, want [32]byte) error {
	signed := t.presign(http.MethodGet, t.objectURL(bucket, key), time.Now().UTC(), 300)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := t.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // tool
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK && sha256.Sum256(got) != want {
		return errContent
	}
	return nil
}

// presign builds a SigV4 query-signed URL with host as the only signed header.
func (t *target) presign(method, rawURL string, now time.Time, expires int) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	scope := sigv4.Scope{Date: now.Format(sigv4.DateFormat), Region: t.region, Service: sigv4.Service}
	q := u.Query()
	q.Set("X-Amz-Algorithm", sigv4.Algorithm)
	q.Set("X-Amz-Credential", t.ak+"/"+scope.String())
	q.Set("X-Amz-Date", now.Format(sigv4.TimeFormat))
	q.Set("X-Amz-Expires", strconv.Itoa(expires))
	q.Set("X-Amz-SignedHeaders", "host")
	canon := sigv4.CanonicalRequest(method, u.EscapedPath(), q, http.Header{}, u.Host, 0, []string{"host"}, sigv4.UnsignedPayload)
	sig := sigv4.Signature(sigv4.SigningKey(t.sk, scope), sigv4.StringToSign(now, scope, canon))
	u.RawQuery = q.Encode() + "&X-Amz-Signature=" + sig
	return u.String()
}
