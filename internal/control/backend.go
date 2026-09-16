package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/s3/s3response"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// maxBackendBody caps a backend response the control plane reads: an error, a versioning status,
// a canary object, or one listing page (ADR-0007). Operator actions only; never client traffic.
const maxBackendBody = 8 << 20

// backend is the control plane's S3 client for one cluster: the proxy's live cluster, with its
// resolved credentials, signing every request with shunt's own signer.
type backend struct{ cl *upstream.Cluster }

type backendReply struct {
	status int
	header http.Header
	body   []byte
	code   string // the XML <Code> of an error body
}

// escapeKey percent-encodes an object key for a request path, keeping its slashes.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = sigv4.Escape(p)
	}
	return strings.Join(parts, "/")
}

// do sends one signed request. The payload hash is the body's own SHA-256.
func (b backend) do(ctx context.Context, method, bucket, key string, query url.Values, body []byte, hdr map[string]string) (backendReply, error) {
	target := "/" + bucket
	if key != "" {
		target += "/" + escapeKey(key)
	}
	if len(query) > 0 {
		target += "?" + strings.ReplaceAll(query.Encode(), "+", "%20")
	}
	sum := sha256.Sum256(body)
	var lastErr error
	for range b.cl.Endpoints {
		ep := b.cl.Next()
		req, err := http.NewRequestWithContext(ctx, method, b.cl.Scheme+"://"+ep+target, bytes.NewReader(body)) //nolint:gosec // G704: a cluster endpoint from the directory
		if err != nil {
			return backendReply{}, err
		}
		req.Host = ep
		req.ContentLength = int64(len(body))
		if len(body) == 0 {
			req.Body = http.NoBody
		}
		req.Header.Set("User-Agent", "shunt-control")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		sigv4.Sign(req, b.cl.Creds, b.cl.Region, hex.EncodeToString(sum[:]), time.Now())
		resp, err := b.cl.Transport.RoundTrip(req)
		if err != nil {
			if upstream.IsConnectError(err) {
				lastErr = err
				continue
			}
			return backendReply{}, fmt.Errorf("%s: %w", b.cl.Name, err)
		}
		data, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBackendBody))
		_ = resp.Body.Close()
		if rerr != nil {
			return backendReply{}, fmt.Errorf("%s: %w", b.cl.Name, rerr)
		}
		r := backendReply{status: resp.StatusCode, header: resp.Header, body: data}
		if resp.StatusCode >= 300 {
			var e struct {
				Code string `xml:"Code"`
			}
			_ = xml.Unmarshal(data, &e) //nolint:errcheck // a non-XML error body just has no code
			r.code = e.Code
		}
		return r, nil
	}
	return backendReply{}, fmt.Errorf("%s: no endpoint answered: %w", b.cl.Name, lastErr)
}

// bucketExists reports whether bucket exists on the cluster.
func (b backend) bucketExists(ctx context.Context, bucket string) (bool, error) {
	r, err := b.do(ctx, http.MethodHead, bucket, "", nil, nil, nil)
	switch {
	case err != nil:
		return false, err
	case r.status == http.StatusOK:
		return true, nil
	case r.status == http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("%s: HEAD bucket %s answered HTTP %d", b.cl.Name, bucket, r.status)
}

// createBucket creates bucket; a bucket this account already owns counts as created.
func (b backend) createBucket(ctx context.Context, bucket string) error {
	r, err := b.do(ctx, http.MethodPut, bucket, "", nil, nil, nil)
	switch {
	case err != nil:
		return err
	case r.status == http.StatusOK, r.status == http.StatusNoContent, r.code == "BucketAlreadyOwnedByYou":
		return nil
	}
	return fmt.Errorf("%s: creating bucket %s: HTTP %d %s", b.cl.Name, bucket, r.status, r.code)
}

// versioning returns the bucket's GetBucketVersioning status: "", "Enabled", or "Suspended".
func (b backend) versioning(ctx context.Context, bucket string) (string, error) {
	r, err := b.do(ctx, http.MethodGet, bucket, "", url.Values{"versioning": {""}}, nil, nil)
	if err != nil {
		return "", err
	}
	if r.status != http.StatusOK {
		return "", fmt.Errorf("%s: GetBucketVersioning %s: HTTP %d %s", b.cl.Name, bucket, r.status, r.code)
	}
	var v struct {
		Status string `xml:"Status"`
	}
	if err := xml.Unmarshal(r.body, &v); err != nil {
		return "", fmt.Errorf("%s: unreadable GetBucketVersioning response: %w", b.cl.Name, err)
	}
	return v.Status, nil
}

// listPage returns one ListObjectsV2 page of decoded keys and the next continuation token ("" at
// the end).
func (b backend) listPage(ctx context.Context, bucket, token string) (keys []string, next string, err error) {
	q := url.Values{"list-type": {"2"}, "encoding-type": {"url"}}
	if token != "" {
		q.Set("continuation-token", token)
	}
	r, err := b.do(ctx, http.MethodGet, bucket, "", q, nil, nil)
	if err != nil {
		return nil, "", err
	}
	if r.status != http.StatusOK {
		return nil, "", fmt.Errorf("%s: listing %s: HTTP %d %s", b.cl.Name, bucket, r.status, r.code)
	}
	var page s3response.ListObjectsV2Result
	if err := xml.Unmarshal(r.body, &page); err != nil {
		return nil, "", fmt.Errorf("%s: listing %s: %w", b.cl.Name, bucket, err)
	}
	for _, o := range page.Contents {
		if o.Key != nil {
			keys = append(keys, s3.DecodeListingKey(*o.Key))
		}
	}
	if page.IsTruncated != nil && *page.IsTruncated && page.NextContinuationToken != nil {
		next = *page.NextContinuationToken
	}
	return keys, next, nil
}

// lister walks a bucket's keys in listing order, one page in memory at a time.
type lister struct {
	b      backend
	bucket string
	page   []string
	next   string
	done   bool
}

func (l *lister) nextKey(ctx context.Context) (key string, ok bool, err error) {
	for len(l.page) == 0 {
		if l.done {
			return "", false, nil
		}
		keys, next, err := l.b.listPage(ctx, l.bucket, l.next)
		if err != nil {
			return "", false, err
		}
		l.page, l.next, l.done = keys, next, next == ""
	}
	k := l.page[0]
	l.page = l.page[1:]
	return k, true, nil
}

// missingOn returns up to limit keys that source holds and primary does not, walking both
// listings in step: memory is two pages, whatever the bucket size.
func missingOn(ctx context.Context, source backend, sourceBucket string, primary backend, primaryBucket string, limit int) ([]string, error) {
	src := &lister{b: source, bucket: sourceBucket}
	dst := &lister{b: primary, bucket: primaryBucket}
	var missing []string
	d, dok, err := dst.nextKey(ctx)
	if err != nil {
		return nil, err
	}
	for {
		s, sok, err := src.nextKey(ctx)
		if err != nil {
			return nil, err
		}
		if !sok {
			return missing, nil
		}
		for dok && d < s {
			if d, dok, err = dst.nextKey(ctx); err != nil {
				return nil, err
			}
		}
		if !dok || d != s {
			missing = append(missing, s)
			if len(missing) >= limit {
				return missing, nil
			}
		}
	}
}

// errNotEmpty is returned when a bucket still holds objects after every delete was issued.
var errNotEmpty = errors.New("bucket is not empty")

// empty aborts every in-progress multipart upload and deletes every object in bucket, then
// confirms the listing is empty. It returns the counts.
func (b backend) empty(ctx context.Context, bucket string) (objects, uploads int, err error) {
	for {
		r, err := b.do(ctx, http.MethodGet, bucket, "", url.Values{"uploads": {""}}, nil, nil)
		if err != nil {
			return objects, uploads, err
		}
		if r.code == "NoSuchUpload" {
			break // an in-memory test backend's answer for a bucket that never had an upload
		}
		if r.status != http.StatusOK {
			return objects, uploads, fmt.Errorf("%s: ListMultipartUploads %s: HTTP %d %s", b.cl.Name, bucket, r.status, r.code)
		}
		var page struct {
			Uploads []struct {
				Key      string `xml:"Key"`
				UploadID string `xml:"UploadId"`
			} `xml:"Upload"`
		}
		if err := xml.Unmarshal(r.body, &page); err != nil {
			return objects, uploads, fmt.Errorf("%s: ListMultipartUploads %s: %w", b.cl.Name, bucket, err)
		}
		if len(page.Uploads) == 0 {
			break
		}
		for _, u := range page.Uploads {
			ar, err := b.do(ctx, http.MethodDelete, bucket, u.Key, url.Values{"uploadId": {u.UploadID}}, nil, nil)
			if err != nil {
				return objects, uploads, err
			}
			if ar.status >= 300 && ar.status != http.StatusNotFound {
				return objects, uploads, fmt.Errorf("%s: aborting upload of %s: HTTP %d %s", b.cl.Name, u.Key, ar.status, ar.code)
			}
			uploads++
		}
	}
	// Deleting while paging with continuation tokens can skip keys, so every round lists from the
	// start until a listing comes back empty.
	for {
		keys, _, err := b.listPage(ctx, bucket, "")
		if err != nil {
			return objects, uploads, err
		}
		if len(keys) == 0 {
			return objects, uploads, nil
		}
		for _, k := range keys {
			dr, err := b.do(ctx, http.MethodDelete, bucket, k, nil, nil, nil)
			if err != nil {
				return objects, uploads, err
			}
			if dr.status >= 300 && dr.status != http.StatusNotFound {
				return objects, uploads, fmt.Errorf("%s: deleting %s: HTTP %d %s: %w", b.cl.Name, k, dr.status, dr.code, errNotEmpty)
			}
			objects++
		}
	}
}

// deleteBucket removes an empty bucket; one that is already gone counts as removed.
func (b backend) deleteBucket(ctx context.Context, bucket string) error {
	r, err := b.do(ctx, http.MethodDelete, bucket, "", nil, nil, nil)
	switch {
	case err != nil:
		return err
	case r.status == http.StatusNoContent, r.status == http.StatusOK, r.code == "NoSuchBucket":
		return nil
	}
	return fmt.Errorf("%s: deleting bucket %s: HTTP %d %s", b.cl.Name, bucket, r.status, r.code)
}
