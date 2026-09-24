package runtimecfg

import (
	"strings"
	"sync"
	"testing"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/upstream"
)

var (
	lineage = directory.Identity{ClusterID: strings.Repeat("a", 32), Epoch: strings.Repeat("1", 32)}
	other   = directory.Identity{ClusterID: strings.Repeat("a", 32), Epoch: strings.Repeat("2", 32)}
)

func bundle(id directory.Identity, version int64) *Bundle {
	return &Bundle{Snapshot: directory.NewSnapshot(&directory.File{Version: version, Identity: id}), Keys: auth.NewTable(nil),
		Clusters: upstream.NewRegistry(upstream.Options{}, nil).Load()}
}

// Bundles move forward within one lineage: an older version is refused, an equal one (a key change
// with no directory write) is taken, a schema-1 directory may take its first identity, and another
// lineage is refused.
func TestPublishOrder(t *testing.T) {
	p := NewPublisher(bundle(directory.Identity{}, 3))
	if err := p.Publish(bundle(lineage, 4)); err != nil {
		t.Fatalf("a schema-1 directory taking its identity: %v", err)
	}
	if err := p.Publish(bundle(lineage, 3)); err == nil || !strings.Contains(err.Error(), "version 4 is published") {
		t.Fatalf("an older version: %v", err)
	}
	if err := p.Publish(bundle(lineage, 4)); err != nil {
		t.Fatalf("the same version again (a key change): %v", err)
	}
	if err := p.Publish(bundle(other, 9)); err == nil || !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("another lineage: %v", err)
	}
	if b := p.Load(); b.Version() != 4 || b.Identity() != lineage {
		t.Fatalf("published: version %d %+v", b.Version(), b.Identity())
	}
}

// Many publishers race: the version published last is the highest, and readers never see one go
// back (-race proves the pointer swap safe).
func TestPublishRace(t *testing.T) {
	p := NewPublisher(bundle(lineage, 0))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		var last int64
		for {
			select {
			case <-stop:
				return
			default:
			}
			v := p.Load().Version()
			if v < last {
				t.Errorf("a reader saw version %d after %d", v, last)
				return
			}
			last = v
		}
	}()
	var pub sync.WaitGroup
	for v := int64(1); v <= 200; v++ {
		pub.Add(1)
		go func() {
			defer pub.Done()
			_ = p.Publish(bundle(lineage, v)) //nolint:errcheck // an older one is refused, by design
		}()
	}
	pub.Wait()
	close(stop)
	wg.Wait()
	if v := p.Load().Version(); v != 200 {
		t.Fatalf("published version %d, want 200", v)
	}
}

// BenchmarkLoad is what every request pays for its bundle.
func BenchmarkLoad(b *testing.B) {
	p := NewPublisher(bundle(lineage, 1))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if p.Load() == nil {
				b.Fatal("no bundle")
			}
		}
	})
}
