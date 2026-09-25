package member

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// Design §8's 100,000-placement snapshot on the wire and on disk: the decode that precedes
// BenchmarkInstall100k's install on a fetch, and the checked decode of the restart cache.

// directory100k is one version of 100,000 placements, as BenchmarkInstall100k builds it.
func directory100k() control.Directory {
	d := control.Directory{File: directory.File{Version: 1, Identity: testIdentity,
		Clusters: map[string]config.Cluster{"vast01": {Type: "s3", Scheme: "http", Region: "r", EndpointMode: "static", Endpoints: []string{"127.0.0.1:1"},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:vast01"}}},
		Tenants: map[string]directory.Tenant{"acme": {DefaultCluster: "vast01"}}, Placements: make(map[string]directory.Placement, 100_000)},
		Secrets: map[string]string{"control:vast01": "s1"}}
	for i := range 100_000 {
		name := fmt.Sprintf("bucket-%06d", i)
		d.Placements["acme/"+name] = directory.Placement{State: directory.StateActive, Primary: "vast01", Names: map[string]string{"vast01": name}}
	}
	return d
}

// BenchmarkDirectoryDecode100k is a member decoding a fetched 100,000-placement directory (the
// json.Unmarshal in Client.call), before it is validated and installed. Bytes/snapshot is the
// response body a fetch of a full version carries to every proxy.
func BenchmarkDirectoryDecode100k(b *testing.B) {
	d := directory100k()
	raw, err := json.Marshal(d)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		var out control.Directory
		if err := json.Unmarshal(raw, &out); err != nil || len(out.Placements) != 100_000 {
			b.Fatalf("decode: %v", err)
		}
	}
	b.ReportMetric(float64(len(raw)), "bytes/snapshot")
}

// BenchmarkCacheDecode100k is a restarting member reading its 100,000-placement restart cache:
// the envelope's decode, schema/protocol/identity checks and checksum (decodeCache), before the
// install. Bytes/snapshot is the cache file's size.
func BenchmarkCacheDecode100k(b *testing.B) {
	c := New(Config{ProxyID: "p1", CacheDir: b.TempDir(), LeaseTTL: time.Minute}, slog.New(slog.DiscardHandler))
	d := directory100k()
	if _, err := c.install(&d, true); err != nil {
		b.Fatal(err)
	}
	raw, err := os.ReadFile(c.cacheFile())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if out, why := c.decodeCache(raw); why != "" || len(out.Placements) != 100_000 {
			b.Fatalf("cache: %s", why)
		}
	}
	b.ReportMetric(float64(len(raw)), "bytes/snapshot")
}
