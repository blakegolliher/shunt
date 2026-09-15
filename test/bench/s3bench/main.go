// Command s3bench measures direct-vs-via-shunt GET and PUT latency and throughput and the proxy's
// CPU-seconds per GiB (docs/DESIGN.md §8 method, POC-1 cuts in docs/POC.md). Test tool only.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type side struct {
	name   string
	client *s3.Client
}

func newSide(name, endpoint, addr, domain, region, ak, sk, caFile string, conns int, insecure bool) (*side, error) {
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
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // explicit -direct-insecure opt-in, temporary
	}
	cfg := aws.Config{
		Region: region, Credentials: credentials.NewStaticCredentialsProvider(ak, sk, ""),
		HTTPClient:                 &http.Client{Transport: tr},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           1,
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	return &side{name: name, client: c}, nil
}

// cpuSeconds reads process_cpu_seconds_total from shunt's admin endpoint.
func cpuSeconds(admin string) float64 {
	resp, err := http.Get(admin + "/-/metrics") //nolint:noctx // tool
	if err != nil {
		return 0
	}
	defer resp.Body.Close() //nolint:errcheck // tool
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "process_cpu_seconds_total ") {
			v, _ := strconv.ParseFloat(strings.TrimPrefix(sc.Text(), "process_cpu_seconds_total "), 64)
			return v
		}
	}
	return 0
}

type stats struct {
	p50, p99 time.Duration
	mbps     float64
	seconds  float64
	ops      int
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	i := int(float64(len(d)-1) * p)
	return d[i]
}

// run performs ops operations of size bytes over conns goroutines and returns latency stats.
func run(ctx context.Context, s *side, bucket, op string, size, conns, ops int, payload []byte) (stats, error) {
	var mu sync.Mutex
	var lat []time.Duration
	var firstErr error
	work := make(chan int, ops)
	for i := 0; i < ops; i++ {
		work <- i
	}
	close(work)
	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				key := fmt.Sprintf("bench/%d-%d", size, i%conns) // conns keys per size, overwritten; GET reads what PUT wrote
				t0 := time.Now()
				var err error
				switch op {
				case "PUT":
					_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(payload), ContentLength: aws.Int64(int64(size))})
				case "GET":
					var out *s3.GetObjectOutput
					out, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
					if err == nil {
						_, err = io.Copy(io.Discard, out.Body)
						_ = out.Body.Close()
					}
				}
				d := time.Since(t0)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	el := time.Since(start)
	if firstErr != nil {
		return stats{}, firstErr
	}
	return stats{p50: percentile(lat, 0.5), p99: percentile(lat, 0.99), seconds: el.Seconds(), ops: ops,
		mbps: float64(size) * float64(ops) / el.Seconds() / (1 << 20)}, nil
}

func main() {
	var (
		directEP = flag.String("direct", "http://shunt.example.com", "backend endpoint URL")
		directAd = flag.String("direct-addr", "127.0.0.1:3900", "backend host:port")
		viaEP    = flag.String("via", "https://shunt.example.com:8443", "shunt endpoint URL")
		viaAd    = flag.String("via-addr", "127.0.0.1:8443", "shunt host:port")
		admin    = flag.String("admin", "http://127.0.0.1:9900", "shunt admin endpoint (CPU seconds)")
		domain   = flag.String("domain", "shunt.example.com", "wildcard base domain")
		region   = flag.String("region", "garage", "signing region")
		caFile   = flag.String("ca", "test/e2e/certs/wildcard.crt", "CA for the shunt endpoint")
		bucket   = flag.String("bucket", "s3bench", "bucket (created, emptied, deleted)")
		matrix   = flag.String("matrix", "4096:1:400,4096:64:3200,1048576:1:200,1048576:64:640,1073741824:1:3", "size:conns:ops entries")
		runs     = flag.Int("runs", 3, "runs per cell; the median is reported")
		backend  = flag.String("backend", "garage", "backend label for the report")
		mode     = flag.String("mode", "passthrough", "passthrough|resign; resign signs the via side with SHUNT_ACCESS_KEY/SHUNT_SECRET")
		insecure = flag.Bool("direct-insecure", false, "skip TLS verification on the direct side (temporary)")
	)
	flag.Parse()
	ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	vak, vsk, vregion := ak, sk, *region
	if *mode == "resign" {
		vak, vsk, vregion = os.Getenv("SHUNT_ACCESS_KEY"), os.Getenv("SHUNT_SECRET"), "us-east-1"
	}
	ctx := context.Background()
	direct, err := newSide("direct", *directEP, *directAd, *domain, *region, ak, sk, "", 64, *insecure)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	via, err := newSide("via", *viaEP, *viaAd, *domain, vregion, vak, vsk, *caFile, 64, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_, _ = direct.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})

	fmt.Printf("backend=%s mode=%s runs=%d (median reported)\n\n", *backend, *mode, *runs)
	fmt.Printf("| size | conns | op | direct p50 | direct p99 | via p50 | via p99 | added p50 | added p99 | direct MiB/s | via MiB/s | ratio | proxy CPU s/GiB | proxy CPU µs/op |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, cell := range strings.Split(*matrix, ",") {
		var size, conns, ops int
		if _, err := fmt.Sscanf(cell, "%d:%d:%d", &size, &conns, &ops); err != nil {
			fmt.Fprintf(os.Stderr, "bad matrix cell %q: %v\n", cell, err)
			os.Exit(2)
		}
		payload := deterministic(size)
		for _, op := range []string{"PUT", "GET"} {
			if op == "GET" { // seed the keys the GETs will read (through direct)
				if _, err := run(ctx, direct, *bucket, "PUT", size, conns, conns, payload); err != nil {
					fmt.Fprintln(os.Stderr, "seed:", err)
					os.Exit(1)
				}
			}
			var ds, vs []stats
			var cpuPerGiB, cpuPerOp []float64
			for r := 0; r < *runs; r++ {
				d, err := run(ctx, direct, *bucket, op, size, conns, ops, payload)
				if err != nil {
					fmt.Fprintln(os.Stderr, "direct:", err)
					os.Exit(1)
				}
				c0 := cpuSeconds(*admin)
				v, err := run(ctx, via, *bucket, op, size, conns, ops, payload)
				if err != nil {
					fmt.Fprintln(os.Stderr, "via:", err)
					os.Exit(1)
				}
				c1 := cpuSeconds(*admin)
				gib := float64(size) * float64(ops) / (1 << 30)
				if gib > 0 {
					cpuPerGiB = append(cpuPerGiB, (c1-c0)/gib)
				}
				cpuPerOp = append(cpuPerOp, (c1-c0)/float64(ops)*1e6)
				ds, vs = append(ds, d), append(vs, v)
			}
			d, v := median(ds), median(vs)
			sort.Float64s(cpuPerGiB)
			sort.Float64s(cpuPerOp)
			cpu, cpuOp := 0.0, 0.0
			if len(cpuPerGiB) > 0 {
				cpu = cpuPerGiB[len(cpuPerGiB)/2]
			}
			if len(cpuPerOp) > 0 {
				cpuOp = cpuPerOp[len(cpuPerOp)/2]
			}
			fmt.Printf("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %.1f | %.1f | %.2f | %.2f | %.0f |\n",
				human(size), conns, op, ms(d.p50), ms(d.p99), ms(v.p50), ms(v.p99), ms(v.p50-d.p50), ms(v.p99-d.p99),
				d.mbps, v.mbps, v.mbps/d.mbps, cpu, cpuOp)
		}
	}
	// Cleanup.
	list, _ := direct.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
	if list != nil {
		for _, o := range list.Contents {
			_, _ = direct.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: o.Key})
		}
	}
	_, _ = direct.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket})
}

func median(s []stats) stats {
	sort.Slice(s, func(i, j int) bool { return s[i].p50 < s[j].p50 })
	return s[len(s)/2]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000) }

func human(n int) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%d GiB", n>>30)
	case n >= 1<<20:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d B", n)
}

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
