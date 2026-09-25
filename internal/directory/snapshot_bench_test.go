package directory

import (
	"fmt"
	"testing"

	"github.com/blakegolliher/shunt/internal/config"
)

// BenchmarkSnapshotFile100k is Snapshot.File on a 100,000-placement snapshot: a deep copy of the
// whole directory. It is the unit cost behind any caller that reads one field (the identity, a
// cluster) through File() rather than an accessor; control's heartbeat handler and barrier
// evaluation, and a member's heartbeat, do so per heartbeat or per poll (see
// control.BenchmarkHeartbeatHandler and member.BenchmarkBeat at placements=100000).
func BenchmarkSnapshotFile100k(b *testing.B) {
	f := &File{Version: 1, Identity: Identity{ClusterID: "c0ffee00c0ffee00c0ffee00c0ffee00", Epoch: "e0000000000000000000000000000001"},
		Clusters: map[string]config.Cluster{"vast01": {}}, Tenants: map[string]Tenant{"acme": {DefaultCluster: "vast01"}},
		Placements: make(map[string]Placement, 100_000)}
	for i := range 100_000 {
		name := fmt.Sprintf("bucket-%06d", i)
		f.Placements["acme/"+name] = Placement{State: StateActive, Primary: "vast01", Names: map[string]string{"vast01": name}}
	}
	snap := NewSnapshot(f)
	b.ReportAllocs()
	for b.Loop() {
		if snap.File().Identity.IsZero() {
			b.Fatal("no identity")
		}
	}
	b.ReportMetric(100_000, "placements/op")
}
