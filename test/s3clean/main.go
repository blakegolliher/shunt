// Command s3clean empties a bucket, through shunt or directly: it lists a page, hands the page's
// keys to workers that delete them (DeleteObjects, or one DeleteObject per key), and repeats full
// listing passes until a pass lists nothing. Test tool only.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func newClient(endpoint, addr, domain, region, ak, sk, caFile string, conns int, insecure, vhost bool) (*s3.Client, error) {
	tr := &http.Transport{
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConnsPerHost: conns * 2,
		MaxIdleConns:        conns * 2,
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
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // explicit -insecure opt-in, test tool
	}
	cfg := aws.Config{
		Region: region, Credentials: credentials.NewStaticCredentialsProvider(ak, sk, ""),
		HTTPClient:                 &http.Client{Transport: tr},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           3,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = !vhost }), nil
}

// cleaner is one bucket's emptying run.
type cleaner struct {
	c        *s3.Client
	bucket   string
	prefix   string
	page     int32
	workers  int
	perKey   bool
	versions bool
	dryRun   bool
	errOut   func(format string, a ...any)
}

// tally is one pass's counts.
type tally struct {
	listed, deleted, failed atomic.Int64
	pages                   atomic.Int64
}

// pass lists the bucket once, start to end, and deletes what each page returned while the next page
// is listed. It returns when the listing is exhausted and every batch has been answered.
func (cl *cleaner) pass(ctx context.Context) (*tally, error) {
	t := &tally{}
	batches := make(chan []types.ObjectIdentifier, cl.workers)
	var wg sync.WaitGroup
	for i := 0; i < cl.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				cl.delete(ctx, b, t)
			}
		}()
	}
	var err error
	if cl.versions {
		err = cl.listVersions(ctx, batches, t)
	} else {
		err = cl.listObjects(ctx, batches, t)
	}
	close(batches)
	wg.Wait()
	return t, err
}

func (cl *cleaner) send(ctx context.Context, batches chan<- []types.ObjectIdentifier, b []types.ObjectIdentifier, t *tally) error {
	t.pages.Add(1)
	t.listed.Add(int64(len(b)))
	if cl.dryRun || len(b) == 0 {
		return nil
	}
	select {
	case batches <- b:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cl *cleaner) listObjects(ctx context.Context, batches chan<- []types.ObjectIdentifier, t *tally) error {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(cl.bucket), MaxKeys: aws.Int32(cl.page)}
	if cl.prefix != "" {
		in.Prefix = aws.String(cl.prefix)
	}
	for {
		out, err := cl.c.ListObjectsV2(ctx, in)
		if err != nil {
			return fmt.Errorf("ListObjectsV2 page %d: %w", t.pages.Load()+1, err)
		}
		b := make([]types.ObjectIdentifier, 0, len(out.Contents))
		for _, o := range out.Contents {
			b = append(b, types.ObjectIdentifier{Key: o.Key})
		}
		if err := cl.send(ctx, batches, b, t); err != nil {
			return err
		}
		if !aws.ToBool(out.IsTruncated) || aws.ToString(out.NextContinuationToken) == "" {
			return nil
		}
		in.ContinuationToken = out.NextContinuationToken
	}
}

func (cl *cleaner) listVersions(ctx context.Context, batches chan<- []types.ObjectIdentifier, t *tally) error {
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(cl.bucket), MaxKeys: aws.Int32(cl.page)}
	if cl.prefix != "" {
		in.Prefix = aws.String(cl.prefix)
	}
	for {
		out, err := cl.c.ListObjectVersions(ctx, in)
		if err != nil {
			return fmt.Errorf("ListObjectVersions page %d: %w", t.pages.Load()+1, err)
		}
		b := make([]types.ObjectIdentifier, 0, len(out.Versions)+len(out.DeleteMarkers))
		for _, v := range out.Versions {
			b = append(b, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			b = append(b, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if err := cl.send(ctx, batches, b, t); err != nil {
			return err
		}
		if !aws.ToBool(out.IsTruncated) {
			return nil
		}
		in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

// delete removes one batch and counts per-key outcomes; a failed key is reported and left for the
// next pass rather than stopping the run.
func (cl *cleaner) delete(ctx context.Context, b []types.ObjectIdentifier, t *tally) {
	if cl.perKey {
		for _, o := range b {
			_, err := cl.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cl.bucket), Key: o.Key, VersionId: o.VersionId})
			if err != nil {
				t.failed.Add(1)
				cl.errOut("delete %s: %v\n", aws.ToString(o.Key), err)
				continue
			}
			t.deleted.Add(1)
		}
		return
	}
	out, err := cl.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(cl.bucket),
		Delete: &types.Delete{Objects: b, Quiet: aws.Bool(true)},
	})
	if err != nil {
		t.failed.Add(int64(len(b)))
		cl.errOut("DeleteObjects (%d keys, first %s): %v\n", len(b), aws.ToString(b[0].Key), err)
		return
	}
	for _, e := range out.Errors {
		cl.errOut("delete %s: %s %s\n", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
	}
	t.failed.Add(int64(len(out.Errors)))
	t.deleted.Add(int64(len(b) - len(out.Errors)))
}

// abortUploads aborts every in-progress multipart upload under the prefix and returns how many.
func (cl *cleaner) abortUploads(ctx context.Context) (int, error) {
	in := &s3.ListMultipartUploadsInput{Bucket: aws.String(cl.bucket)}
	if cl.prefix != "" {
		in.Prefix = aws.String(cl.prefix)
	}
	n := 0
	for {
		out, err := cl.c.ListMultipartUploads(ctx, in)
		if err != nil {
			return n, fmt.Errorf("ListMultipartUploads: %w", err)
		}
		for _, u := range out.Uploads {
			if cl.dryRun {
				n++
				continue
			}
			_, err := cl.c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(cl.bucket), Key: u.Key, UploadId: u.UploadId})
			if err != nil {
				cl.errOut("abort %s %s: %v\n", aws.ToString(u.Key), aws.ToString(u.UploadId), err)
				continue
			}
			n++
		}
		if !aws.ToBool(out.IsTruncated) {
			return n, nil
		}
		in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
}

// run passes over the bucket until a pass lists nothing or rounds run out. It reports whether the
// bucket (under the prefix) was seen empty.
func (cl *cleaner) run(ctx context.Context, rounds int, report func(round int, t *tally, d time.Duration)) (bool, error) {
	for round := 1; round <= rounds; round++ {
		start := time.Now()
		t, err := cl.pass(ctx)
		report(round, t, time.Since(start))
		if err != nil {
			return false, err
		}
		if t.listed.Load() == 0 {
			return true, nil
		}
		if cl.dryRun {
			return false, nil
		}
	}
	return false, nil
}

func main() { os.Exit(clean()) }

// clean is main with an exit code, so deferred cleanup runs before the process exits.
func clean() int {
	var (
		endpoint = flag.String("endpoint", "https://shunt.example.com:8443", "S3 endpoint URL (shunt, or a backend directly)")
		addr     = flag.String("addr", "127.0.0.1:8443", "host:port to dial for hosts under -domain (empty: resolve normally)")
		domain   = flag.String("domain", "shunt.example.com", "wildcard base domain that -addr applies to")
		region   = flag.String("region", "us-east-1", "signing region")
		caFile   = flag.String("ca", "test/e2e/certs/wildcard.crt", "CA for the endpoint (empty: system roots)")
		insecure = flag.Bool("insecure", false, "skip TLS verification")
		vhost    = flag.Bool("vhost", false, "virtual-hosted addressing (default path style)")
		akEnv    = flag.String("access-key-env", "AWS_ACCESS_KEY_ID", "env var holding the access key")
		skEnv    = flag.String("secret-key-env", "AWS_SECRET_ACCESS_KEY", "env var holding the secret key")
		bucket   = flag.String("bucket", "", "bucket to empty (required)")
		prefix   = flag.String("prefix", "", "only delete keys under this prefix")
		page     = flag.Int("page", 1000, "keys per list page and per DeleteObjects batch (S3 max 1000)")
		workers  = flag.Int("workers", 8, "concurrent delete batches")
		rounds   = flag.Int("rounds", 10, "maximum full listing passes before giving up")
		perKey   = flag.Bool("per-key", false, "one DeleteObject per key instead of DeleteObjects")
		versions = flag.Bool("versions", false, "list with ListObjectVersions and delete every version and delete marker")
		uploads  = flag.Bool("uploads", false, "also abort in-progress multipart uploads")
		dropBkt  = flag.Bool("delete-bucket", false, "delete the bucket once it is empty")
		dryRun   = flag.Bool("dry-run", false, "list and count only; delete nothing")
		timeout  = flag.Duration("timeout", 0, "give up after this long (0: no limit)")
	)
	flag.Parse()
	if *bucket == "" || *page < 1 || *page > 1000 || *workers < 1 || *rounds < 1 {
		fmt.Fprintln(os.Stderr, "s3clean: -bucket is required; -page must be 1..1000; -workers and -rounds must be >= 1")
		return 2
	}
	ak, sk := os.Getenv(*akEnv), os.Getenv(*skEnv)
	if ak == "" || sk == "" {
		fmt.Fprintf(os.Stderr, "s3clean: set %s and %s\n", *akEnv, *skEnv)
		return 2
	}
	c, err := newClient(*endpoint, *addr, *domain, *region, ak, sk, *caFile, *workers, *insecure, *vhost)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s3clean:", err)
		return 2
	}
	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	var errMu sync.Mutex
	cl := &cleaner{
		c: c, bucket: *bucket, prefix: *prefix, page: int32(*page), workers: *workers, //nolint:gosec // G115: bounded to 1..1000 above
		perKey: *perKey, versions: *versions, dryRun: *dryRun,
		errOut: func(format string, a ...any) {
			errMu.Lock()
			defer errMu.Unlock()
			fmt.Fprintf(os.Stderr, format, a...)
		},
	}

	if *uploads {
		n, abortErr := cl.abortUploads(ctx)
		verb := "aborted"
		if *dryRun {
			verb = "found"
		}
		fmt.Printf("multipart uploads %s: %d\n", verb, n)
		if abortErr != nil {
			fmt.Fprintln(os.Stderr, "s3clean:", abortErr)
			return 1
		}
	}

	total := int64(0)
	empty, err := cl.run(ctx, *rounds, func(round int, t *tally, d time.Duration) {
		total += t.deleted.Load()
		rate := 0.0
		if d > 0 {
			rate = float64(t.deleted.Load()) / d.Seconds()
		}
		fmt.Printf("round %d: pages=%d listed=%d deleted=%d failed=%d in %s (%.0f deletes/s)\n",
			round, t.pages.Load(), t.listed.Load(), t.deleted.Load(), t.failed.Load(), d.Round(time.Millisecond), rate)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "s3clean:", err)
		return 1
	}
	switch {
	case *dryRun:
		fmt.Println("dry run: nothing deleted")
		return 0
	case !empty:
		fmt.Fprintf(os.Stderr, "s3clean: %s still lists objects after %d rounds (%d deleted)\n", *bucket, *rounds, total)
		return 1
	}
	fmt.Printf("%s empty: %d deleted\n", *bucket, total)
	if *dropBkt {
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); err != nil {
			var apiErr interface{ ErrorCode() string }
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "BucketNotEmpty" && !*versions {
				fmt.Fprintln(os.Stderr, "s3clean: bucket not empty; it may hold old versions (rerun with -versions)")
			}
			fmt.Fprintln(os.Stderr, "s3clean: DeleteBucket:", err)
			return 1
		}
		fmt.Printf("%s deleted\n", *bucket)
	}
	return 0
}
