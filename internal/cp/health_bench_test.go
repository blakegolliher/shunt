package cp

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// healthBenchRig is this node and 29 peers (T19's 30 members) whose probes answer at once, as the
// member each is: a cycle's cost apart from the network.
func healthBenchRig(b *testing.B) *healthRig {
	peers := make([]localMember, 0, 29)
	for i := range 29 {
		id := uint64(0x100 + i)
		peers = append(peers, localMember{id: id, name: fmt.Sprintf("m%x", id), peerURLs: []string{fmt.Sprintf("http://m%x:2380", id)}})
	}
	return newHealthRig(b, func(_ context.Context, api string) (LocalStatus, error) {
		return LocalStatus{MemberID: strings.TrimSuffix(strings.TrimPrefix(api, "http://m"), ":9901"), Leader: "1"}, nil
	}, peers...)
}

// BenchmarkHealthSnapshot30 is what one status read costs for health with 30 members: the local
// membership and each member's last observation. It never probes, so it is all a viewer adds.
func BenchmarkHealthSnapshot30(b *testing.B) {
	rg := healthBenchRig(b)
	rg.h.Cycle(context.Background())
	for b.Loop() {
		if ms, _, _ := rg.h.Snapshot(0); len(ms) != 30 {
			b.Fatalf("%d members", len(ms))
		}
	}
}

// BenchmarkHealthCycle30 is one sampling cycle over 30 members, five probes at a time, with the
// quorum read through this node's real etcd member: the sampler's own work every 5 s.
func BenchmarkHealthCycle30(b *testing.B) {
	rg := healthBenchRig(b)
	ctx := context.Background()
	for b.Loop() {
		rg.h.Cycle(ctx)
	}
	b.ReportMetric(float64(rg.h.probes.Load())/float64(b.N), "probes/cycle")
}
