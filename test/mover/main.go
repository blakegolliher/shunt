// Command mover copies a bucket's objects from a migrating placement's source cluster to its
// primary. It implements the mover contract of docs/DESIGN.md §2.5 with the guards of ADR-0004:
//
//   - it refuses to run unless the placement is MIGRATING, or RAMPING at ratio 1, so it never
//     copies a key whose writes still go to the source;
//   - it never overwrites a newer client write: If-None-Match: * where the target honors it,
//     and a HEAD-then-commit guard where it does not (Garage);
//   - it re-HEADs the source after each copy and removes its own copy if the object was deleted
//     while in flight, so a delete is never resurrected;
//   - it reproduces the source's multipart part layout, so ETags survive;
//   - it keeps a resumable cursor and appends a JSONL ledger.
//
// It is a test fixture, not the product: a production mover has the same obligations.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
)

// inlineLimit is the largest object the mover holds in memory; anything larger is streamed
// through a temporary file (single-part sources) or copied part by part (multipart sources).
const inlineLimit = 16 << 20

type side struct {
	name   string
	bucket string
	cl     *s3.Client
}

type job struct {
	key         string
	tenant      string
	client      string
	src, dst    side
	conditional bool
}

// ledgerLine is one row of the run's record, in the order the mover wrote them.
type ledgerLine struct {
	At       time.Time `json:"at"`
	Bucket   string    `json:"bucket"`
	Key      string    `json:"key"`
	Size     int64     `json:"size"`
	SrcETag  string    `json:"src_etag"`
	DstETag  string    `json:"dst_etag,omitempty"`
	Parts    int32     `json:"parts,omitempty"`
	Drift    bool      `json:"etag_drift,omitempty"`
	Mode     string    `json:"mode"`             // conditional | guarded
	Result   string    `json:"result"`           // copied | skipped | vanished | error
	Detail   string    `json:"detail,omitempty"` // why it was skipped, or the error
	Duration string    `json:"took,omitempty"`
}

type stats struct {
	copied, skipped, vanished, failed, drifted int
	bytes                                      int64
}

func main() {
	var (
		cfgPath  = flag.String("config", "test/e2e/data/shunt-mixed.yaml", "shunt config naming the clusters and the directory")
		bucket   = flag.String("bucket", "", "one placement to move, as <tenant>/<bucket>")
		from     = flag.String("from", "", "move every MIGRATING placement whose source is this cluster")
		stateDir = flag.String("state-dir", "test/e2e/data", "where cursors and the ledger are written")
		ledgerTo = flag.String("ledger-bucket", "", "backend bucket on the target cluster to upload the ledger to")
		dryRun   = flag.Bool("dry-run", false, "list what would be copied and stop")
	)
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		die("config: %v", err)
	}
	dir, err := directory.Load(cfg.Directory.File, cfg.Clusters)
	if err != nil {
		die("directory: %v", err)
	}
	jobs, err := selectPlacements(dir, cfg, *bucket, *from)
	if err != nil {
		die("%v", err)
	}
	if len(jobs) == 0 {
		die("nothing to move: no MIGRATING placement matched (use -bucket <tenant>/<bucket> or -from <cluster>)")
	}

	ctx := context.Background()
	total := stats{}
	for i := range jobs {
		j := jobs[i]
		fmt.Printf("== %s/%s: %s/%s → %s/%s (%s guard)\n", j.tenant, j.client,
			j.src.name, j.src.bucket, j.dst.name, j.dst.bucket, guardName(j.conditional))
		s, err := move(ctx, j, *stateDir, *ledgerTo, *dryRun)
		total.copied += s.copied
		total.skipped += s.skipped
		total.vanished += s.vanished
		total.failed += s.failed
		total.drifted += s.drifted
		total.bytes += s.bytes
		if err != nil {
			fmt.Fprintf(os.Stderr, "mover: %s/%s: %v\n", j.tenant, j.client, err)
			total.failed++
		}
	}
	fmt.Printf("\nmover: %d copied, %d already on the target, %d vanished mid-copy, %d failed, %s moved\n",
		total.copied, total.skipped, total.vanished, total.failed, humanBytes(total.bytes))
	if total.drifted > 0 {
		fmt.Fprintf(os.Stderr, "mover: %d objects changed ETag across the move\n", total.drifted)
		os.Exit(1)
	}
	if total.failed > 0 {
		os.Exit(1)
	}
}

func guardName(conditional bool) string {
	if conditional {
		return "If-None-Match"
	}
	return "HEAD-then-commit"
}

// selectPlacements finds the placements to move and refuses the ones that are not ready.
func selectPlacements(dir *directory.File, cfg *config.Config, one, from string) ([]job, error) {
	clients := map[string]*s3.Client{}
	client := func(name string) (*s3.Client, error) {
		if c, ok := clients[name]; ok {
			return c, nil
		}
		cc, ok := cfg.Clusters[name]
		if !ok {
			return nil, fmt.Errorf("cluster %q is not configured", name)
		}
		secret, err := config.ResolveSecret(cc.Credentials.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("cluster %s: %w", name, err)
		}
		c := s3.NewFromConfig(aws.Config{
			Region:                     cc.Region,
			Credentials:                credentials.NewStaticCredentialsProvider(cc.Credentials.AccessKey, secret, ""),
			RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
			ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
			RetryMaxAttempts:           3,
		}, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cc.Scheme + "://" + cc.Endpoints[0])
			o.UsePathStyle = true
		})
		clients[name] = c
		return c, nil
	}

	keys := make([]string, 0, len(dir.Placements))
	for k := range dir.Placements {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var jobs []job
	for _, key := range keys {
		p := dir.Placements[key]
		tenant, name, _ := strings.Cut(key, "/")
		switch {
		case one != "" && key != one:
			continue
		case one == "" && from != "" && p.Source != from:
			continue
		case one == "" && from == "":
			continue
		}
		// The mover never runs while writes are still split: a key copied to the target while the
		// source still takes its writes would read stale (docs/DESIGN.md §2.5).
		switch {
		case p.State == directory.StateMigrating:
		case p.State == directory.StateRamping && p.Ramp != nil && p.Ramp.Ratio >= 1:
		default:
			if one != "" {
				return nil, fmt.Errorf("%s is %s: the mover runs on a MIGRATING placement, or a RAMPING one at ratio 1", key, p.State)
			}
			continue
		}
		srcCl, err := client(p.Source)
		if err != nil {
			return nil, err
		}
		dstCl, err := client(p.Primary)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job{
			tenant: tenant, client: name,
			src:         side{name: p.Source, bucket: p.Names[p.Source], cl: srcCl},
			dst:         side{name: p.Primary, bucket: p.Names[p.Primary], cl: dstCl},
			conditional: cfg.Clusters[p.Primary].Capabilities.ConditionalWriteOr(true),
		})
	}
	return jobs, nil
}

// move copies one placement's objects, resuming from its cursor.
func move(ctx context.Context, j job, stateDir, ledgerBucket string, dryRun bool) (stats, error) {
	var s stats
	safe := strings.ReplaceAll(j.tenant+"-"+j.client, "/", "-")
	cursorPath := filepath.Join(stateDir, "mover-"+safe+".cursor")
	ledgerPath := filepath.Join(stateDir, "mover-"+safe+".ledger.jsonl")
	after := readCursor(cursorPath)
	if after != "" {
		fmt.Printf("   resuming after %q\n", after)
	}
	ledger, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return s, err
	}
	defer ledger.Close() //nolint:errcheck // flushed per line

	var token *string
	for {
		page, err := j.src.cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &j.src.bucket, ContinuationToken: token, StartAfter: optional(after),
		})
		if err != nil {
			return s, fmt.Errorf("list %s: %w", j.src.bucket, err)
		}
		for i := range page.Contents {
			key := aws.ToString(page.Contents[i].Key)
			if dryRun {
				fmt.Printf("   would copy %s (%d bytes)\n", key, aws.ToInt64(page.Contents[i].Size))
				s.copied++
				continue
			}
			line := copyOne(ctx, j, key)
			line.At, line.Bucket = time.Now().UTC(), j.tenant+"/"+j.client
			enc, _ := json.Marshal(line) //nolint:errcheck // a struct of scalars
			_, _ = ledger.Write(append(enc, '\n'))
			switch line.Result {
			case "copied":
				s.copied++
				s.bytes += line.Size
				if line.Drift {
					s.drifted++
					fmt.Fprintf(os.Stderr, "   %s: ETag changed across the move: %s -> %s\n", key, line.SrcETag, line.DstETag)
				}
			case "skipped":
				s.skipped++
			case "vanished":
				s.vanished++
			default:
				s.failed++
				fmt.Fprintf(os.Stderr, "   %s: %s\n", key, line.Detail)
			}
			writeCursor(cursorPath, key)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token, after = page.NextContinuationToken, ""
	}
	// The pass reached the end of the bucket, so the cursor has nothing left to resume: remove it.
	// Keeping it would make the next pass start after the last key of this one and silently skip
	// everything a client wrote in the meantime under a lower-sorting prefix.
	_ = os.Remove(cursorPath)
	fmt.Printf("   %d copied, %d already there, %d vanished, %d failed, %s\n", s.copied, s.skipped, s.vanished, s.failed, humanBytes(s.bytes))
	if ledgerBucket != "" && !dryRun {
		if err := uploadLedger(ctx, j, ledgerBucket, ledgerPath); err != nil {
			fmt.Fprintf(os.Stderr, "   ledger upload: %v\n", err)
		}
	}
	return s, nil
}

// copyOne copies a single object under the guard the target's capability profile allows.
func copyOne(ctx context.Context, j job, key string) ledgerLine {
	started := time.Now()
	line := ledgerLine{Key: key, Mode: guardName(j.conditional)}
	defer func() { line.Duration = time.Since(started).Round(time.Millisecond).String() }()

	head, err := j.src.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.src.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			line.Result, line.Detail = "vanished", "gone from the source before the copy started"
			return line
		}
		line.Result, line.Detail = "error", err.Error()
		return line
	}
	line.Size, line.SrcETag = aws.ToInt64(head.ContentLength), aws.ToString(head.ETag)
	line.Parts, err = sourceParts(ctx, j, key, head)
	if err != nil {
		line.Result, line.Detail = "error", err.Error()
		return line
	}

	// Look before writing. Without conditional writes this HEAD is the guard (ADR-0004); with them
	// the backend refuses the write itself and the HEAD is only an optimisation, but an important
	// one: a second pass over a migrated bucket would otherwise re-upload every object's bytes to
	// be told 412.
	switch _, herr := j.dst.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.dst.bucket, Key: &key}); {
	case herr == nil:
		line.Result, line.Detail = "skipped", "already on the target"
		return line
	case !isNotFound(herr):
		line.Result, line.Detail = "error", herr.Error()
		return line
	}

	var etag string
	if line.Parts > 1 {
		etag, err = copyMultipart(ctx, j, key, head, line.Parts)
	} else {
		etag, err = copySingle(ctx, j, key, head)
	}
	switch {
	case err != nil && isPreconditionFailed(err):
		line.Result, line.Detail = "skipped", "a client wrote it to the target first (412)"
		return line
	case err != nil && isNotFound(err):
		line.Result, line.Detail = "vanished", "gone from the source mid-copy"
		return line
	case err != nil:
		line.Result, line.Detail = "error", err.Error()
		return line
	}
	line.DstETag = etag
	// The whole point of reproducing the part layout is that the ETag survives the move. If it did
	// not, the object is still correct but no longer addressable by the client's cached ETag, so
	// the ledger records it rather than letting it pass silently.
	if etag != "" && line.SrcETag != "" && etag != line.SrcETag {
		line.Drift = true
	}

	// The delete/copy race: if the object went away while we were copying it, take our copy back
	// out rather than resurrect it (docs/DESIGN.md §2.5, ADR-0004 race 1).
	if _, serr := j.src.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.src.bucket, Key: &key}); serr != nil && isNotFound(serr) {
		if _, derr := j.dst.cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &j.dst.bucket, Key: &key}); derr != nil {
			line.Result, line.Detail = "error", "deleted from the source mid-copy and the copy could not be removed: "+derr.Error()
			return line
		}
		line.Result, line.Detail = "vanished", "deleted from the source mid-copy; the copy was removed"
		return line
	}
	line.Result = "copied"
	return line
}

// sourceParts reports how many parts the source object was uploaded in. A plain HEAD does not
// answer that on every backend — MinIO omits x-amz-mp-parts-count unless a part is named, and a
// multipart object copied as one part comes out with a different ETag — so the multipart ETag
// suffix is the trigger and a HEAD of part 1 is the authority.
func sourceParts(ctx context.Context, j job, key string, head *s3.HeadObjectOutput) (int32, error) {
	if n := aws.ToInt32(head.PartsCount); n > 0 {
		return n, nil
	}
	etag := strings.Trim(aws.ToString(head.ETag), `"`)
	i := strings.LastIndexByte(etag, '-')
	if i < 0 {
		return 1, nil
	}
	n, err := strconv.Atoi(etag[i+1:])
	if err != nil || n < 1 {
		return 1, nil
	}
	ph, err := j.src.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.src.bucket, Key: &key, PartNumber: aws.Int32(1)})
	if err != nil {
		return 0, fmt.Errorf("reading the part layout of %s: %w", key, err)
	}
	if c := aws.ToInt32(ph.PartsCount); c > 0 {
		return c, nil
	}
	return int32(n), nil //nolint:gosec // G115: a part count, bounded by S3 at 10000
}

// copySingle streams a one-part object. Objects up to inlineLimit go through memory; larger ones
// through a temporary file, so the PUT stays a single part and the ETag is preserved.
func copySingle(ctx context.Context, j job, key string, head *s3.HeadObjectOutput) (string, error) {
	get, err := j.src.cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &j.src.bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer get.Body.Close() //nolint:errcheck // copied below
	size := aws.ToInt64(head.ContentLength)

	in := &s3.PutObjectInput{
		Bucket: &j.dst.bucket, Key: &key, ContentLength: aws.Int64(size),
		ContentType: head.ContentType, Metadata: head.Metadata, CacheControl: head.CacheControl,
		ContentEncoding: head.ContentEncoding, ContentDisposition: head.ContentDisposition,
	}
	if j.conditional {
		in.IfNoneMatch = aws.String("*")
	}
	if size <= inlineLimit {
		body, rerr := io.ReadAll(get.Body)
		if rerr != nil {
			return "", rerr
		}
		in.Body = bytes.NewReader(body)
	} else {
		tmp, terr := os.CreateTemp("", "shunt-mover-*")
		if terr != nil {
			return "", terr
		}
		defer os.Remove(tmp.Name()) //nolint:errcheck // best effort
		defer tmp.Close()           //nolint:errcheck // best effort
		if _, cerr := io.Copy(tmp, get.Body); cerr != nil {
			return "", cerr
		}
		if _, serr := tmp.Seek(0, io.SeekStart); serr != nil {
			return "", serr
		}
		in.Body = tmp
	}
	out, err := j.dst.cl.PutObject(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// copyMultipart reproduces the source's part layout, so the object keeps its ETag. Part sizes come
// from the source itself: HEAD with a part number reports that part's length.
func copyMultipart(ctx context.Context, j job, key string, head *s3.HeadObjectOutput, parts int32) (string, error) {
	create, err := j.dst.cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &j.dst.bucket, Key: &key, ContentType: head.ContentType, Metadata: head.Metadata,
	})
	if err != nil {
		return "", err
	}
	uploadID := create.UploadId
	abort := func() {
		_, _ = j.dst.cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &j.dst.bucket, Key: &key, UploadId: uploadID})
	}

	var completed []types.CompletedPart
	var offset int64
	for n := int32(1); n <= parts; n++ {
		ph, perr := j.src.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.src.bucket, Key: &key, PartNumber: aws.Int32(n)})
		if perr != nil {
			abort()
			return "", fmt.Errorf("part %d of %d: %w", n, parts, perr)
		}
		size := aws.ToInt64(ph.ContentLength)
		rng := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
		get, gerr := j.src.cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &j.src.bucket, Key: &key, Range: &rng})
		if gerr != nil {
			abort()
			return "", gerr
		}
		body, rerr := io.ReadAll(get.Body)
		_ = get.Body.Close()
		if rerr != nil {
			abort()
			return "", rerr
		}
		up, uerr := j.dst.cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: &j.dst.bucket, Key: &key, UploadId: uploadID, PartNumber: aws.Int32(n),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		if uerr != nil {
			abort()
			return "", uerr
		}
		completed = append(completed, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(n)})
		offset += size
	}

	// The guard sits immediately before the commit: the parts are staged, so the window in which a
	// client write could be clobbered is one round trip, whatever the object's size (ADR-0004).
	in := &s3.CompleteMultipartUploadInput{
		Bucket: &j.dst.bucket, Key: &key, UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}
	if j.conditional {
		in.IfNoneMatch = aws.String("*")
	} else if _, herr := j.dst.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.dst.bucket, Key: &key}); herr == nil {
		abort()
		return "", &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "a client wrote it while the parts were uploading"}
	} else if !isNotFound(herr) {
		abort()
		return "", herr
	}
	out, err := j.dst.cl.CompleteMultipartUpload(ctx, in)
	if err != nil {
		abort()
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func uploadLedger(ctx context.Context, j job, bucket, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only
	key := fmt.Sprintf("mover/%s-%s-%s.jsonl", j.tenant, j.client, time.Now().UTC().Format("20060102T150405Z"))
	_, err = j.dst.cl.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: f})
	if err == nil {
		fmt.Printf("   ledger: s3://%s/%s\n", bucket, key)
	}
	return err
}

func readCursor(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var c struct {
		Key string `json:"key"`
	}
	if json.Unmarshal(b, &c) != nil {
		return ""
	}
	return c.Key
}

func writeCursor(path, key string) {
	b, _ := json.Marshal(map[string]string{"key": key, "at": time.Now().UTC().Format(time.RFC3339)}) //nolint:errcheck // scalars
	_ = os.WriteFile(path, b, 0o644)                                                                 //nolint:gosec // a cursor file, no secrets
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func isNotFound(err error) bool {
	var nf *types.NoSuchKey
	var nb *types.NoSuchBucket
	if errors.As(err, &nf) || errors.As(err, &nb) {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "NotFound" || api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "404")
}

func isPreconditionFailed(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "412")
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "mover: "+format+"\n", a...)
	os.Exit(2)
}
