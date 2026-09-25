package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
	"github.com/blakegolliher/shunt/internal/telemetry"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// Design §3 of the hardening plan: under a held (and uncertain) bucket, an unrelated ACTIVE
// bucket must see no global serialization and no full-directory scan per request. These run the
// resign-mode small GET and PUT on bucket X (t/bbb, ACTIVE) through the handler with admission
// gates wired as serve wires them, while another bucket Y (t/yyy) is:
//
//	none           not held: no barrier anywhere
//	held           held: its mutations gate closed by a barrier
//	held+uncertain held, and its gate has counted a backend outcome never learned
//	10k/100held    10,000 placements in the directory, 100 of them held, Y uncertain too
//
// X's cost must be the same in every row.

// holdVariant is one row above.
type holdVariant struct {
	name       string
	extra      int  // placements besides X and Y
	held       int  // of those, how many carry a barrier (Y always does when hold is set)
	hold       bool // Y carries a barrier
	uncertainY bool // Y's gate has an uncertain outcome counted
}

var holdVariants = []holdVariant{
	{name: "none"},
	{name: "held", hold: true},
	{name: "held+uncertain", hold: true, uncertainY: true},
	{name: "10k/100held", extra: 9998, held: 99, hold: true, uncertainY: true},
}

// yBarrier is the barrier holding Y.
const yBarrier = "1758800000000-yyyyyy"

// holdRig builds the handler for one variant against backend be.
func holdRig(b *testing.B, be *httptest.Server, v holdVariant) (*Handler, *admission.Gates) {
	b.Helper()
	sha, trailer := true, true
	clusters := map[string]config.Cluster{"test": {
		Type: "s3", Scheme: "http", Region: clusterRegion, EndpointMode: "static", Endpoints: []string{strings.TrimPrefix(be.URL, "http://")},
		Credentials:  config.Credentials{AccessKey: clusterAK, SecretRef: "env:UNUSED"},
		Capabilities: config.Capabilities{EnforcesSHA256: &sha, UnsignedTrailer: &trailer},
	}}
	set := upstream.NewRegistry(upstream.Options{}, func(string) (string, error) { return clusterSecret, nil })
	if _, _, err := set.Apply(clusters); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(set.Close)
	var body strings.Builder
	body.WriteString("version: 1\ntenants: { t: { default_cluster: test } }\nplacements:\n")
	body.WriteString("  t/bbb: { state: ACTIVE, primary: test, names: { test: bbb } }\n")
	if v.hold {
		body.WriteString("  t/yyy: { state: ACTIVE, primary: test, names: { test: yyy }, barrier: { id: " + yBarrier + ", kind: mutations } }\n")
	} else {
		body.WriteString("  t/yyy: { state: ACTIVE, primary: test, names: { test: yyy } }\n")
	}
	for i := range v.extra {
		name := fmt.Sprintf("b%06d", i)
		barrier := ""
		if i < v.held {
			barrier = fmt.Sprintf(", barrier: { id: 1758800000000-%06x, kind: mutations }", i)
		}
		fmt.Fprintf(&body, "  t/%s: { state: ACTIVE, primary: test, names: { test: %s }%s }\n", name, name, barrier)
	}
	path := filepath.Join(b.TempDir(), "directory.yaml")
	writeDirectory(b, path, body.String(), clusters)
	dir, err := directory.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	rt, gates := followWithGates(b, dir, mapStore{clientAK: {AccessKey: clientAK, Secret: clientSecret, Tenant: "t"}}, set)
	if v.uncertainY {
		// A write to Y whose outcome was never learned, taken before the hold closed its gate.
		gates.Open("t/yyy", admission.Mutations)
		tok, _, _ := gates.Enter("t/yyy", admission.Mutations)
		tok.Release(gates, admission.Uncertain)
		gates.Close("t/yyy", admission.Mutations, yBarrier)
		if gates.Uncertain() != 1 {
			b.Fatal("no uncertain outcome counted")
		}
	}
	h := New(Handler{Runtime: rt, Dir: dir, Gates: gates, Rewrite: true,
		Metrics: telemetry.NewMetrics(), Access: telemetry.NewAccessLogger(nil), Slow: telemetry.NewSlowRing(100, time.Second),
		IdleTimeout: time.Second, MetadataTimeout: time.Second, Mode: ModeResign}, 256<<10)
	if st := gates.State("t/yyy"); (st.Closed[admission.Mutations] != "") != v.hold {
		b.Fatalf("Y's gate: %+v, hold %v", st, v.hold)
	}
	return h, gates
}

// signedPUT is a client PUT of payload to path, signed with the payload's hash.
func signedPUT(path string, payload []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(payload))
	req.RequestURI = path
	sum := sha256.Sum256(payload)
	clientSign(req, hex.EncodeToString(sum[:]))
	return req
}

// BenchmarkResignSmallGETUnderHold is BenchmarkResignSmallGET (a 4 KiB object) on ACTIVE bucket
// X while another bucket is held, in each holdVariant. A read of an ACTIVE bucket takes no
// admission token, so what this shows is that nothing else on its path looks at the barriers.
func BenchmarkResignSmallGETUnderHold(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4096")
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer be.Close()
	for _, v := range holdVariants {
		b.Run(v.name, func(b *testing.B) {
			h, _ := holdRig(b, be, v)
			req := httptest.NewRequest(http.MethodGet, "/bbb/k", nil)
			req.RequestURI = "/bbb/k"
			clientSign(req, sigv4.UnsignedPayload)
			b.ReportAllocs()
			for b.Loop() {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatal(rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkResignSmallPUTUnderHold is a 4 KiB signed PUT on ACTIVE bucket X while another bucket
// is held, in each holdVariant. A PUT takes and gives back X's mutations token, so this is the
// admission path itself next to a closed gate: X's gate shares no lock with Y's, and nothing
// scans the directory's barriers per request. Each variant first checks that a PUT to Y is
// refused, so the hold is real.
func BenchmarkResignSmallPUTUnderHold(b *testing.B) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer be.Close()
	payload := bytes.Repeat([]byte("p"), 4096)
	for _, v := range holdVariants {
		b.Run(v.name, func(b *testing.B) {
			h, gates := holdRig(b, be, v)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, signedPUT("/yyy/k", payload))
			if held := rec.Code == http.StatusServiceUnavailable; held != v.hold {
				b.Fatalf("PUT to Y: %d, hold %v: %s", rec.Code, v.hold, rec.Body.String())
			}
			req := signedPUT("/bbb/k", payload)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				req.Body = io.NopCloser(bytes.NewReader(payload))
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatal(rec.Code, rec.Body.String())
				}
			}
			if st := gates.State("t/bbb"); st.Inflight[admission.Mutations] != 0 || st.Uncertain[admission.Mutations] != 0 {
				b.Fatalf("X's gate after the run: %+v", st)
			}
		})
	}
}
