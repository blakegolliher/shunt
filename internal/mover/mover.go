// Package mover copies a moving placement's objects from its source cluster to its primary for
// `shunt migrate run` and control-node browser operations. It never runs on the proxy request path
// (docs/DESIGN.md §2.5, ADR-0004, ADR-0009). It keeps the obligations the test/mover fixture proved
// in POC-4:
//
//   - it refuses to run unless the placement is MIGRATING, or RAMPING at ratio 1, so it never
//     copies a key whose writes still go to the source;
//   - it never overwrites a newer client write: If-None-Match: * where the target honors it,
//     and a HEAD-then-commit guard where it does not, only when the operator accepted that window;
//   - it re-HEADs the source after each copy and removes its own copy if the object was deleted
//     while in flight, so a delete is not resurrected; it removes only its own copy, with If-Match
//     on the ETag its PUT returned where the target honors it, and otherwise after a HEAD shows that
//     ETag and a Last-Modified no later than the PUT (ADR-0004 race 1);
//   - it reproduces the source's multipart part layout, so ETags survive;
//   - it keeps a resumable cursor, appends a JSONL ledger, and reports each pass to the control API.
package mover

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	s3err "github.com/blakegolliher/shunt/internal/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// inlineLimit is the largest object the mover holds in memory; anything larger is streamed
// through a temporary file (single-part sources) or copied part by part (multipart sources).
const inlineLimit = 16 << 20

type side struct {
	name      string
	bucket    string
	cl        *s3.Client
	accessKey string
	secretRef string
}

type job struct {
	key         string
	tenant      string
	client      string
	src, dst    side
	conditional bool // the target honors If-None-Match: * on PUT
	condDelete  bool // the target honors If-Match on DELETE
	// keep, for a move of part of a bucket (ADR-0018 N3), is the moving range: the source leg also
	// holds keys it keeps, which are never copied. Nil copies every key.
	keep func(key string) bool
}

// LedgerEntry is one row of a run's append-only record, in the order the mover wrote it.
type LedgerEntry struct {
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

func guardName(conditional bool) string {
	if conditional {
		return "If-None-Match"
	}
	return "HEAD-then-commit"
}

func withdrawName(condDelete bool) string {
	if condDelete {
		return "If-Match"
	}
	return "re-HEAD"
}

// selectPlacements finds the placements to move and refuses the ones that are not ready. A target
// that ignores If-None-Match: * is refused unless acceptLoss, with the message shunt migrate start
// gives: the check lives in both, because the mover can also run on a placement RAMPING at ratio 1,
// which migrate start never saw. Any refusal stops the whole run before a byte is copied.
func selectPlacements(dir *directory.File, secrets map[string]string, one, from string, acceptLoss bool) ([]job, error) {
	clients := map[string]*s3.Client{}
	client := func(name string) (*s3.Client, error) {
		if c, ok := clients[name]; ok {
			return c, nil
		}
		cc, ok := dir.Clusters[name]
		if !ok {
			return nil, fmt.Errorf("cluster %q is not configured", name)
		}
		// A control: secret came with the placement from the control plane (ADR-0015); the
		// others resolve on this host, as they do for shunt serve.
		secret, ok := secrets[cc.Credentials.SecretRef]
		if !ok {
			var err error
			if secret, err = config.ResolveSecret(cc.Credentials.SecretRef); err != nil {
				return nil, fmt.Errorf("cluster %s: %w", name, err)
			}
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
		var keep func(string) bool
		if m := p.Move; m != nil {
			// Part of a bucket moves: the move's two legs, and only the keys in its range (ADR-0018 N3).
			rg := m.Range
			keep = func(k string) bool { return migrate.InRangeHash(rg, k) }
			p = p.MoveView()
		}
		tenant, name, _ := strings.Cut(key, "/")
		switch {
		case one != "" && key != one:
			continue
		case one == "" && from != "" && p.ClusterOf(p.Source) != from:
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
		// Roles name buckets; ClusterOf names the cluster each is on (a move's two legs may share one).
		srcName, dstName := p.ClusterOf(p.Source), p.ClusterOf(p.Primary)
		srcCl, err := client(srcName)
		if err != nil {
			return nil, err
		}
		dstCl, err := client(dstName)
		if err != nil {
			return nil, err
		}
		if !dir.Clusters[dstName].Capabilities.ConditionalWriteOr(true) && !acceptLoss {
			return nil, migrate.RefuseLostWriteWindow(key, dstName)
		}
		jobs = append(jobs, job{
			tenant: tenant, client: name,
			src:         side{name: srcName, bucket: p.Names[p.Source], cl: srcCl, accessKey: dir.Clusters[srcName].Credentials.AccessKey, secretRef: dir.Clusters[srcName].Credentials.SecretRef},
			dst:         side{name: dstName, bucket: p.Names[p.Primary], cl: dstCl, accessKey: dir.Clusters[dstName].Credentials.AccessKey, secretRef: dir.Clusters[dstName].Credentials.SecretRef},
			conditional: dir.Clusters[dstName].Capabilities.ConditionalWriteOr(true),
			// Never assumed: a target that ignores If-Match on DELETE would delete a newer write.
			condDelete: dir.Clusters[dstName].Capabilities.ConditionalDeleteOr(false),
			keep:       keep,
		})
	}
	return jobs, nil
}

// move copies one placement's objects, resuming from its cursor.
func move(ctx context.Context, j job, paths Paths, dryRun bool, out, errOut io.Writer, report func(s stats, lastKey string, done bool)) (stats, error) {
	var s stats
	safe := strings.ReplaceAll(j.tenant+"-"+j.client, "/", "-")
	cursorPath := filepath.Join(paths.CursorDir, "mover-"+safe+".cursor")
	ledgerPath := filepath.Join(paths.LedgerDir, "mover-"+safe+".ledger.jsonl")
	ledgerBucket := paths.LedgerBucket
	after := readCursor(cursorPath)
	if after != "" {
		_, _ = fmt.Fprintf(out, "   resuming after %q\n", after)
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
			if j.keep != nil && !j.keep(key) {
				continue // a key the source leg keeps
			}
			if dryRun {
				_, _ = fmt.Fprintf(out, "   would copy %s (%d bytes)\n", key, aws.ToInt64(page.Contents[i].Size))
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
					_, _ = fmt.Fprintf(errOut, "   %s: ETag changed across the move: %s -> %s\n", key, line.SrcETag, line.DstETag)
				}
			case "skipped":
				s.skipped++
			case "vanished":
				s.vanished++
			default:
				s.failed++
				_, _ = fmt.Fprintf(errOut, "   %s: %s\n", key, line.Detail)
			}
			writeCursor(cursorPath, key)
			if n := s.copied + s.skipped + s.vanished + s.failed; report != nil && n%1000 == 0 {
				report(s, key, false)
			}
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
	if report != nil && !dryRun {
		report(s, "", true)
	}
	_, _ = fmt.Fprintf(out, "   %d copied, %d already there, %d vanished, %d failed, %s\n", s.copied, s.skipped, s.vanished, s.failed, HumanBytes(s.bytes))
	if ledgerBucket != "" && !dryRun {
		if err := uploadLedger(ctx, j, ledgerBucket, ledgerPath, out); err != nil {
			_, _ = fmt.Fprintf(errOut, "   ledger upload: %v\n", err)
		}
	}
	return s, nil
}

// copyOne copies a single object under the guard the target's capability profile allows.
func copyOne(ctx context.Context, j job, key string) LedgerEntry {
	started := time.Now()
	line := LedgerEntry{Key: key, Mode: guardName(j.conditional)}
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
	var putDate time.Time
	if line.Parts > 1 {
		etag, putDate, err = copyMultipart(ctx, j, key, head, line.Parts)
	} else {
		etag, putDate, err = copySingle(ctx, j, key, head)
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
	// out rather than resurrect it (docs/DESIGN.md §2.5, ADR-0004 race 1). Only our copy: a client
	// may have written the key again since our PUT landed.
	if _, serr := j.src.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.src.bucket, Key: &key}); serr != nil && isNotFound(serr) {
		detail, werr := withdraw(ctx, j, key, etag, putDate)
		if werr != nil {
			line.Result, line.Detail = "error", "deleted from the source mid-copy and the copy could not be removed: "+werr.Error()
			return line
		}
		line.Result, line.Detail = "vanished", "deleted from the source mid-copy; "+detail
		return line
	}
	line.Result = "copied"
	return line
}

// withdraw removes the mover's own copy of key from the target, and nothing a client wrote after
// it. With conditional_delete it is one DELETE with If-Match on the ETag the mover's PUT returned.
// Without it, a HEAD must show that ETag and a Last-Modified no later than the PUT's Date before an
// unconditional DELETE; a client write that lands between that HEAD and the DELETE is still lost,
// and so is a client write of identical bytes within the same second (ADR-0004 race 1).
func withdraw(ctx context.Context, j job, key, etag string, putDate time.Time) (string, error) {
	if j.condDelete {
		_, err := j.dst.cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &j.dst.bucket, Key: &key, IfMatch: aws.String(etag)})
		switch {
		case err == nil:
			return "the copy was removed (If-Match)", nil
		case isPreconditionFailed(err):
			return "a newer client write had replaced the copy and was kept (412)", nil
		case isNotFound(err):
			return "the copy was already gone", nil
		}
		return "", err
	}
	head, err := j.dst.cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &j.dst.bucket, Key: &key})
	switch {
	case err != nil && isNotFound(err):
		return "the copy was already gone", nil
	case err != nil:
		return "", err
	case !ownCopy(aws.ToString(head.ETag), etag, aws.ToTime(head.LastModified), putDate):
		return "the target holds a newer client write, which was kept", nil
	}
	if _, err := j.dst.cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &j.dst.bucket, Key: &key}); err != nil {
		return "", err
	}
	return "the copy was removed (re-HEAD)", nil
}

// ownCopy reports whether the object a HEAD found is the mover's copy: it carries the ETag the
// mover's PUT returned and was not modified after that PUT's Date. Both times come from the
// backend's clock at one-second resolution; with no Date the ETag alone decides.
func ownCopy(headETag, putETag string, modified, putDate time.Time) bool {
	if headETag == "" || headETag != putETag {
		return false
	}
	return putDate.IsZero() || !modified.After(putDate)
}

// responseDate is the Date header of an SDK response, the backend's clock at the moment it answered.
func responseDate(md middleware.Metadata) time.Time {
	if raw, ok := awsmiddleware.GetRawResponse(md).(*smithyhttp.Response); ok {
		if t, err := http.ParseTime(raw.Header.Get("Date")); err == nil {
			return t
		}
	}
	return time.Time{}
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
func copySingle(ctx context.Context, j job, key string, head *s3.HeadObjectOutput) (string, time.Time, error) {
	get, err := j.src.cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &j.src.bucket, Key: &key})
	if err != nil {
		return "", time.Time{}, err
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
			return "", time.Time{}, rerr
		}
		in.Body = bytes.NewReader(body)
	} else {
		tmp, terr := os.CreateTemp("", "shunt-mover-*")
		if terr != nil {
			return "", time.Time{}, terr
		}
		defer os.Remove(tmp.Name()) //nolint:errcheck // best effort
		defer tmp.Close()           //nolint:errcheck // best effort
		if _, cerr := io.Copy(tmp, get.Body); cerr != nil {
			return "", time.Time{}, cerr
		}
		if _, serr := tmp.Seek(0, io.SeekStart); serr != nil {
			return "", time.Time{}, serr
		}
		in.Body = tmp
	}
	out, err := j.dst.cl.PutObject(ctx, in)
	if err != nil {
		return "", time.Time{}, err
	}
	return aws.ToString(out.ETag), responseDate(out.ResultMetadata), nil
}

// copyMultipart reproduces the source's part layout, so the object keeps its ETag. Part sizes come
// from the source itself: HEAD with a part number reports that part's length.
func copyMultipart(ctx context.Context, j job, key string, head *s3.HeadObjectOutput, parts int32) (string, time.Time, error) {
	create, err := j.dst.cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &j.dst.bucket, Key: &key, ContentType: head.ContentType, Metadata: head.Metadata,
	})
	if err != nil {
		return "", time.Time{}, err
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
			return "", time.Time{}, fmt.Errorf("part %d of %d: %w", n, parts, perr)
		}
		size := aws.ToInt64(ph.ContentLength)
		rng := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
		get, gerr := j.src.cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &j.src.bucket, Key: &key, Range: &rng})
		if gerr != nil {
			abort()
			return "", time.Time{}, gerr
		}
		body, rerr := io.ReadAll(get.Body)
		_ = get.Body.Close()
		if rerr != nil {
			abort()
			return "", time.Time{}, rerr
		}
		up, uerr := j.dst.cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: &j.dst.bucket, Key: &key, UploadId: uploadID, PartNumber: aws.Int32(n),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		if uerr != nil {
			abort()
			return "", time.Time{}, uerr
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
		return "", time.Time{}, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "a client wrote it while the parts were uploading"}
	} else if !isNotFound(herr) {
		abort()
		return "", time.Time{}, herr
	}
	out, err := j.dst.cl.CompleteMultipartUpload(ctx, in)
	if err != nil {
		abort()
		return "", time.Time{}, err
	}
	return aws.ToString(out.ETag), responseDate(out.ResultMetadata), nil
}

func uploadLedger(ctx context.Context, j job, bucket, path string, out io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only
	key := fmt.Sprintf("mover/%s-%s-%s.jsonl", j.tenant, j.client, time.Now().UTC().Format("20060102T150405Z"))
	_, err = j.dst.cl.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: f})
	if err == nil {
		_, _ = fmt.Fprintf(out, "   ledger: s3://%s/%s\n", bucket, key)
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

// checkSide lists one key of a side's bucket and turns a signature or unknown-key answer into an
// error that says where the mover's secret comes from: the mover resolves secret_refs in its own
// process, so a terminal with a stale env var fails here while shunt serve works. Other errors
// (AccessDenied on a target the key may write but not list) are left to the copy that follows.
func checkSide(ctx context.Context, role string, sd side) error {
	_, err := sd.cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(sd.bucket), MaxKeys: aws.Int32(1)})
	var api smithy.APIError
	if !errors.As(err, &api) {
		return nil
	}
	switch s3err.ClassifyCredentialError(api.ErrorCode(), api.ErrorMessage()) {
	case s3err.FaultSignature:
		msg := fmt.Sprintf("%s cluster %s rejected the mover's signature (%s): access key %s exists, but the secret this process reads from %s is not that key's secret",
			role, sd.name, api.ErrorCode(), sd.accessKey, sd.secretRef)
		if strings.HasPrefix(sd.secretRef, "env:") {
			msg += "; the mover reads " + strings.TrimPrefix(sd.secretRef, "env:") + " from this terminal's environment, which must hold the same secret shunt serve uses"
		}
		return errors.New(msg)
	case s3err.FaultUnknownKey:
		return fmt.Errorf("%s cluster %s does not know access key %s (%s): the key is recorded on the cluster; re-add it with `shunt cluster add`", role, sd.name, sd.accessKey, api.ErrorCode())
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "412")
}

// HumanBytes formats a mover byte count for operator output.
func HumanBytes(n int64) string {
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
