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

// At the bound, a publish waits as the pending bundle, a newer one replaces it (installs coalesce),
// and the first retired bundle to drain publishes it. Requests are never cut short.
func TestRetiredBound(t *testing.T) {
	var seen []Stats
	p := NewPublisher(bundle(lineage, 1))
	p.Observe = func(st Stats) { seen = append(seen, st) }
	var held []*Bundle
	for v := int64(2); v <= MaxRetired+1; v++ {
		held = append(held, p.Acquire()) // a long request on the current bundle
		if err := p.Publish(bundle(lineage, v)); err != nil {
			t.Fatal(err)
		}
	}
	if st := p.Stats(); st.Version != MaxRetired+1 || st.Retired != MaxRetired || st.Pending != 0 {
		t.Fatalf("at the bound: %+v", st)
	}
	held = append(held, p.Acquire())
	if err := p.Publish(bundle(lineage, 20)); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(bundle(lineage, 21)); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(bundle(lineage, 20)); err == nil {
		t.Fatal("a version older than the pending one was taken")
	}
	if st := p.Stats(); st.Version != MaxRetired+1 || st.Pending != 21 {
		t.Fatalf("past the bound, the proxy serves the old version and 21 waits: %+v", st)
	}
	if b := p.Acquire(); b.Version() != MaxRetired+1 {
		t.Fatalf("a new request took version %d while 21 was pending", b.Version())
	} else {
		b.Release()
	}
	held[0].Release()
	if st := p.Stats(); st.Version != 21 || st.Pending != 0 || st.Retired != MaxRetired {
		t.Fatalf("after one drained: %+v", st)
	}
	for _, b := range held[1:] {
		b.Release()
	}
	if st := p.Stats(); st.Retired != 0 {
		t.Fatalf("all released: %+v", st)
	}
	if last := seen[len(seen)-1]; last.Retired != 0 || last.Pending != 0 || last.Version != 21 {
		t.Fatalf("observed last: %+v", last)
	}
}

// A retired bundle releases its cluster set when its last request ends, not when it is replaced.
func TestBundleHoldsItsClusters(t *testing.T) {
	reg := upstream.NewRegistry(upstream.Options{}, nil)
	set := reg.Load()
	p := NewPublisher(&Bundle{Snapshot: directory.NewSnapshot(&directory.File{Version: 1, Identity: lineage}), Keys: auth.NewTable(nil), Clusters: set})
	held := p.Acquire()
	if _, _, err := reg.Apply(nil); err != nil { // the registry lets go of set
		t.Fatal(err)
	}
	if err := p.Refresh(directory.NewSnapshot(&directory.File{Version: 2, Identity: lineage}), auth.NewTable(nil), reg.Load()); err != nil {
		t.Fatal(err)
	}
	if !set.Retain() {
		t.Fatal("the held bundle's set was released while a request held it")
	}
	set.Release()
	held.Release()
	if set.Retain() {
		t.Fatal("the drained bundle kept its set")
	}
	if err := p.Refresh(directory.NewSnapshot(&directory.File{Version: 3, Identity: lineage}), auth.NewTable(nil), set); err == nil {
		t.Fatal("a bundle on a released set was published")
	}
}

// Requests acquire and release while installs publish, past the bound: every acquired bundle is
// live (count above zero) and, once all requests end, nothing is retired or pending (-race).
func TestAcquireRace(t *testing.T) {
	p := NewPublisher(bundle(lineage, 0))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := p.Acquire()
				if b.refs.Load() < 1 {
					t.Error("acquired a drained bundle")
				}
				b.Release()
			}
		}()
	}
	for v := int64(1); v <= 500; v++ {
		if err := p.Publish(bundle(lineage, v)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if st := p.Stats(); st.Retired != 0 || st.Pending != 0 || st.Version != 500 {
		t.Fatalf("after the race: %+v", st)
	}
}

// BenchmarkLoad is what a status read pays for the bundle.
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

// BenchmarkAcquire is what every request pays for its bundle: acquire and release.
func BenchmarkAcquire(b *testing.B) {
	p := NewPublisher(bundle(lineage, 1))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Acquire().Release()
		}
	})
}
