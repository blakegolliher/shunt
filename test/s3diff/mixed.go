package main

// Mixed-backend mode (POC-3): one shunt endpoint in front of several clusters, with each bucket
// compared against the cluster that actually holds it, under its backend name. On top of the
// direct-vs-via diff it makes four assertions the diff cannot make, because a leak looks the same
// on both sides of a single-backend comparison (docs/STATUS.md):
//
//  1. no via response names a backend bucket or a cluster endpoint;
//  2. <Location> in CompleteMultipartUpload is the client-facing URL;
//  3. every via uploadId carries a cluster prefix;
//  4. ListBuckets is the tenant's directory set and spans more than one cluster, and no tenant
//     sees another tenant's bucket.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// mixedNormalise maps backend bucket names and cluster endpoints to what a client sees, so the
// direct side of the comparison reads like the via side. It is applied to bodies and headers.
var mixedNormalise *strings.Replacer

var (
	locationRe = regexp.MustCompile(`<Location>([^<]*)</Location>`)
	uploadIDRe = regexp.MustCompile(`<UploadId>([^<]*)</UploadId>`)
)

type tenantCreds struct{ ak, sk string }

// mixedBucket is one bucket under test: its client name, the cluster holding it, and the two
// targets that address it.
type mixedBucket struct {
	tenant, client, cluster, backend string
	direct, via                      *target
}

func loadTenantCreds(path string) (map[string]tenantCreds, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f auth.File
	if uerr := yaml.Unmarshal(data, &f); uerr != nil {
		return nil, uerr
	}
	out := map[string]tenantCreds{}
	for _, e := range f.Credentials {
		secret := e.Secret
		if secret == "" && e.SecretRef != "" {
			if secret, err = config.ResolveSecret(e.SecretRef); err != nil {
				return nil, err
			}
		}
		if _, dup := out[e.Tenant]; !dup {
			out[e.Tenant] = tenantCreds{e.AccessKey, secret}
		}
	}
	return out, nil
}

// runMixed runs the mixed-backend matrix and returns the process exit code.
func runMixed(ctx context.Context, cfgPath, viaEP, viaAddr, domain, caFile string, sizes []int) int {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mixed: config:", err)
		return 2
	}
	if cfg.Auth.Mode != "resign" || cfg.Directory.File == "" {
		fmt.Fprintln(os.Stderr, "mixed: needs a resign config with a directory file")
		return 2
	}
	tenants, err := loadTenantCreds(cfg.Auth.CredentialsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mixed: credentials:", err)
		return 2
	}
	vias := map[string]*target{}
	for tenant, c := range tenants {
		t, terr := newTarget("via-"+tenant, viaEP, viaAddr, domain, "us-east-1", c.ak, c.sk, caFile, true, false)
		if terr != nil {
			fmt.Fprintln(os.Stderr, "mixed:", terr)
			return 2
		}
		vias[tenant] = t
	}

	// Pre-seeded placements: make sure the backend bucket exists, so a fresh lab passes.
	seed, err := directory.Load(cfg.Directory.File)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mixed: directory:", err)
		return 2
	}
	// One direct target per cluster, addressing it path-style with its own credentials.
	directs := map[string]*target{}
	var endpoints []string
	for _, name := range sortedKeys(seed.Clusters) {
		cc := seed.Clusters[name]
		secret, serr := config.ResolveSecret(cc.Credentials.SecretRef)
		if serr != nil {
			fmt.Fprintf(os.Stderr, "mixed: cluster %s: %v\n", name, serr)
			return 2
		}
		ep := cc.Endpoints[0]
		endpoints = append(endpoints, ep)
		t, terr := newTarget("direct-"+name, cc.Scheme+"://"+ep, "", domain, cc.Region, cc.Credentials.AccessKey, secret, cc.TLS.CA, true, cc.TLS.InsecureSkipVerify)
		if terr != nil {
			fmt.Fprintln(os.Stderr, "mixed:", terr)
			return 2
		}
		directs[name] = t
	}
	for _, key := range sortedKeys(seed.Placements) {
		p := seed.Placements[key]
		d := directs[p.Primary]
		name := p.Names[p.Primary]
		if _, herr := d.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &name}); herr != nil {
			if _, cerr := d.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &name}); cerr != nil {
				fmt.Fprintf(os.Stderr, "mixed: seeding %s (%s on %s): %v\n", key, name, p.Primary, cerr)
				return 2
			}
			fmt.Fprintf(os.Stderr, "mixed: created seed bucket %s on %s for %s\n", name, p.Primary, key)
		}
		d.rec.take()
	}

	// Only tenants the directory knows take part: a credentials file may also hold tenants for the
	// single-backend runs.
	for tenant := range tenants {
		if _, known := seed.Tenants[tenant]; !known {
			delete(tenants, tenant)
			delete(vias, tenant)
		}
	}
	if len(tenants) == 0 {
		fmt.Fprintln(os.Stderr, "mixed: no credential names a tenant in", cfg.Directory.File)
		return 2
	}

	// One bucket per tenant, created through shunt: it lands on that tenant's default cluster.
	var runID [4]byte
	_, _ = rand.Read(runID[:])
	fresh := "s3diff-" + hex.EncodeToString(runID[:])
	for tenant := range tenants {
		if _, cerr := vias[tenant].client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &fresh}); cerr != nil {
			fmt.Fprintf(os.Stderr, "mixed: CreateBucket %s as %s: %v\n", fresh, tenant, cerr)
			return 2
		}
		vias[tenant].rec.take()
	}
	// The directory now names every bucket's cluster and backend name.
	dir, err := directory.Load(cfg.Directory.File)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mixed: directory:", err)
		return 2
	}
	var buckets []mixedBucket
	var pairs []string
	for _, key := range sortedKeys(dir.Placements) {
		p := dir.Placements[key]
		tenant, client, _ := strings.Cut(key, "/")
		if _, known := tenants[tenant]; !known {
			continue
		}
		cluster := p.Primary
		buckets = append(buckets, mixedBucket{tenant: tenant, client: client, cluster: cluster, backend: p.Names[cluster], direct: directs[cluster], via: vias[tenant]})
		pairs = append(pairs, p.Names[cluster], client)
	}
	if len(buckets) < 2 {
		fmt.Fprintln(os.Stderr, "mixed: fewer than two placements to compare")
		return 2
	}
	for _, ep := range endpoints {
		pairs = append(pairs, "http://"+ep, viaEP, "https://"+ep, viaEP, ep, strings.TrimPrefix(strings.TrimPrefix(viaEP, "https://"), "http://"))
	}
	mixedNormalise = strings.NewReplacer(pairs...)
	defer func() { mixedNormalise = nil }()

	var results []result
	failed, leaks := 0, 0
	viaHost := viaEP
	for _, b := range buckets {
		bucket := b // capture
		run := func(name, op string, compareBody bool, f func(*target, string) error) {
			r := result{name: bucket.tenant + "/" + bucket.client + "/" + name, op: op}
			_ = f(bucket.direct, bucket.backend)
			r.direct = bucket.direct.rec.take()
			_ = f(bucket.via, bucket.client)
			r.via = bucket.via.rec.take()
			r.compare(compareBody)
			if ls := leakCheck(r.via, buckets, endpoints, viaHost); len(ls) > 0 {
				r.diffs = append(r.diffs, ls...)
				leaks += len(ls)
			}
			if len(r.diffs) > 0 {
				failed++
			}
			results = append(results, r)
		}
		runMixedCases(ctx, run, sizes)
	}

	// ListBuckets and cross-tenant isolation: assertions on the via side only.
	asserts, aFailed := mixedAssertions(ctx, vias, buckets)
	failed += aFailed

	fmt.Printf("%-52s %-24s %-9s %-9s %s\n", "case", "op", "direct", "via", "result")
	for _, r := range results {
		res := "ok"
		if len(r.diffs) > 0 {
			res = "DIFF"
		}
		fmt.Printf("%-52s %-24s %-9s %-9s %s\n", r.name, r.op, st(r.direct), st(r.via), res)
		for _, d := range r.diffs {
			fmt.Printf("    %s\n", d)
		}
	}
	fmt.Println()
	for _, a := range asserts {
		fmt.Println(a)
	}
	fmt.Printf("\nmixed: %d placements over %d clusters, %d cases, %d diffs (%d of them leaks)\n",
		len(buckets), len(directs), len(results), failed, leaks)
	for _, b := range buckets {
		fmt.Printf("  %-28s → %-8s %s\n", b.tenant+"/"+b.client, b.cluster, b.backend)
	}
	if failed > 0 {
		return 1
	}
	return 0
}

// runMixedCases is the operation matrix, run once per bucket. f receives the target and the name
// that target uses for the bucket.
func runMixedCases(ctx context.Context, run func(name, op string, body bool, f func(*target, string) error), sizes []int) {
	for kname, key := range trickyKeys {
		for _, size := range sizes {
			if kname != "plain" && size > 4096 {
				continue
			}
			payload := deterministic(size)
			sum := sha256.Sum256(payload)
			k := fmt.Sprintf("%s-%d", key, size)
			label := fmt.Sprintf("%s/%d", kname, size)
			run(label, "PutObject", false, func(t *target, b string) error {
				_, err := t.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b, Key: &k, Body: bytes.NewReader(payload), ContentLength: aws.Int64(int64(size))})
				return err
			})
			run(label, "HeadObject", false, func(t *target, b string) error {
				_, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b, Key: &k})
				return err
			})
			run(label, "GetObject", true, func(t *target, b string) error {
				out, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b, Key: &k})
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
	run("list", "ListObjectsV2", true, func(t *target, b string) error {
		_, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &b, Prefix: aws.String("dir")})
		return err
	})
	run("list", "ListObjectsV2(delim)", true, func(t *target, b string) error {
		_, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &b, Delimiter: aws.String("/"), MaxKeys: aws.Int32(3)})
		return err
	})
	run("list", "ListObjects", true, func(t *target, b string) error {
		_, err := t.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: &b, Prefix: aws.String("dir/")})
		return err
	})
	mpKey := "dir/multipart.bin"
	part1, part2 := deterministic(5<<20), deterministic(1<<20)
	run("multipart", "CreateMultipartUpload", true, func(t *target, b string) error {
		out, err := t.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &b, Key: &mpKey})
		if err != nil {
			return err
		}
		uid := out.UploadId
		p1, err := t.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &b, Key: &mpKey, UploadId: uid, PartNumber: aws.Int32(1), Body: bytes.NewReader(part1), ContentLength: aws.Int64(int64(len(part1)))})
		if err != nil {
			return err
		}
		p2, err := t.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &b, Key: &mpKey, UploadId: uid, PartNumber: aws.Int32(2), Body: bytes.NewReader(part2), ContentLength: aws.Int64(int64(len(part2)))})
		if err != nil {
			return err
		}
		if _, err = t.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &b}); err != nil {
			return err
		}
		if _, err = t.client.ListParts(ctx, &s3.ListPartsInput{Bucket: &b, Key: &mpKey, UploadId: uid}); err != nil {
			return err
		}
		_, err = t.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &b, Key: &mpKey, UploadId: uid,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: p1.ETag, PartNumber: aws.Int32(1)}, {ETag: p2.ETag, PartNumber: aws.Int32(2)}}}})
		return err
	})
	run("multipart", "AbortMultipartUpload", false, func(t *target, b string) error {
		out, err := t.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &b, Key: aws.String("dir/aborted.bin")})
		if err != nil {
			return err
		}
		_, err = t.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &b, Key: aws.String("dir/aborted.bin"), UploadId: out.UploadId})
		return err
	})
	run("copy", "CopyObject", true, func(t *target, b string) error {
		_, err := t.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &b, Key: aws.String("dir/copied.bin"), CopySource: aws.String(b + "/dir/plain.bin-4096")})
		return err
	})
	run("errors", "GetObject(404)", true, func(t *target, b string) error {
		_, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b, Key: aws.String("does/not/exist")})
		return err
	})
	run("delete", "DeleteObject", false, func(t *target, b string) error {
		k := "dir/todelete.bin"
		if _, err := t.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b, Key: &k, Body: bytes.NewReader([]byte("x"))}); err != nil {
			return err
		}
		t.rec.take() // setup
		_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b, Key: &k})
		return err
	})
}

// leakCheck is assertion 1, 2, and 3: a via response must never name a backend bucket or a cluster
// endpoint, its <Location> must be the client-facing URL, and its uploadIds must carry a prefix.
func leakCheck(via []*probe, buckets []mixedBucket, endpoints []string, viaEP string) []string {
	var out []string
	for _, p := range via {
		hay := string(p.body)
		for k, vv := range p.headers {
			hay += "\n" + k + ": " + strings.Join(vv, ",")
		}
		for _, b := range buckets {
			if b.backend != b.client && strings.Contains(hay, b.backend) {
				out = append(out, "LEAK: backend bucket name "+b.backend+" in a via response")
			}
		}
		for _, ep := range endpoints {
			if strings.Contains(hay, ep) {
				out = append(out, "LEAK: cluster endpoint "+ep+" in a via response")
			}
		}
		for _, m := range locationRe.FindAllStringSubmatch(string(p.body), -1) {
			if !strings.HasPrefix(m[1], viaEP+"/") {
				out = append(out, "LEAK: <Location> "+m[1]+" does not start with "+viaEP)
			}
		}
		for _, m := range uploadIDRe.FindAllStringSubmatch(string(p.body), -1) {
			if m[1] == "" {
				continue
			}
			id, _, ok := strings.Cut(m[1], "~")
			if !ok || len(id) != 6 {
				out = append(out, "uploadId "+m[1]+" carries no cluster prefix")
			}
		}
	}
	return out
}

// mixedAssertions is assertion 4: ListBuckets is the tenant's directory set, spans more than one
// cluster, and never shows another tenant's bucket; and a cross-tenant GET is refused.
func mixedAssertions(ctx context.Context, vias map[string]*target, buckets []mixedBucket) (lines []string, failed int) {
	add := func(ok bool, format string, a ...any) {
		mark := "ok  "
		if !ok {
			mark, failed = "FAIL", failed+1
		}
		lines = append(lines, mark+" "+fmt.Sprintf(format, a...))
	}
	for tenant, via := range vias {
		want, clusters := []string{}, map[string]bool{}
		for _, b := range buckets {
			if b.tenant == tenant {
				want = append(want, b.client)
				clusters[b.cluster] = true
			}
		}
		sort.Strings(want)
		res, err := via.client.ListBuckets(ctx, &s3.ListBucketsInput{})
		via.rec.take()
		if err != nil {
			add(false, "%s: ListBuckets: %v", tenant, err)
			continue
		}
		got := make([]string, 0, len(res.Buckets))
		for _, b := range res.Buckets {
			got = append(got, aws.ToString(b.Name))
		}
		sort.Strings(got)
		add(strings.Join(got, ",") == strings.Join(want, ","), "%s: ListBuckets = %v (directory says %v)", tenant, got, want)
		add(len(clusters) > 1, "%s: its buckets span %d clusters", tenant, len(clusters))
	}
	for _, b := range buckets {
		for tenant, via := range vias {
			if tenant == b.tenant {
				continue
			}
			if ownsName(buckets, tenant, b.client) {
				continue // both tenants have a bucket of this name; each sees its own
			}
			_, err := via.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &b.client})
			via.rec.take()
			add(err != nil, "%s cannot reach %s's bucket %s", tenant, b.tenant, b.client)
		}
	}
	// Every via uploadId prefix must name a real cluster id.
	for _, b := range buckets {
		add(upstream.ClusterID(b.cluster) != "", "%s is addressed by the opaque cluster id %s", b.cluster, upstream.ClusterID(b.cluster))
	}
	return lines, failed
}

// ownsName reports whether the tenant has a placement under this client bucket name.
func ownsName(buckets []mixedBucket, tenant, client string) bool {
	for _, b := range buckets {
		if b.tenant == tenant && b.client == client {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
