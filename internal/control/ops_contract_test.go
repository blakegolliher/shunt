package control_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/control/opstest"
	"github.com/blakegolliher/shunt/internal/directory"
)

// The lab's in-memory store passes the operation transaction contract over a directory file.
func TestMemOperationsContract(t *testing.T) {
	opstest.Run(t, func(t *testing.T, o opstest.Options) opstest.Harness {
		path := filepath.Join(t.TempDir(), "directory.yaml")
		if err := os.WriteFile(path, []byte("version: 0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		d, err := directory.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return opstest.Harness{Ops: &control.MemOperations{Dir: d, Limit: o.Limit, Capacity: o.Capacity}, Dir: d}
	})
}

// BenchmarkMemOperationsCreateUpdate is one scoped operation's life in the lab's store, with a
// thousand ended records behind it: create (the conflict scan), start, end.
func BenchmarkMemOperationsCreateUpdate(b *testing.B) {
	m := &control.MemOperations{Limit: 1000, Capacity: 1 << 20}
	for i := range 1000 {
		op := &control.Operation{ID: fmt.Sprintf("0-%06d", i), Status: control.StatusPending, Sequence: 1,
			Scope: &control.Scope{Resource: directory.PlacementResource(fmt.Sprintf("acme/b%d", i)), Clusters: []string{"c1"}}}
		if err := m.Create(b.Context(), op); err != nil {
			b.Fatal(err)
		}
		op.Status, op.Sequence = control.StatusSucceeded, 2
		if err := m.Update(b.Context(), op); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; b.Loop(); i++ {
		op := &control.Operation{ID: fmt.Sprintf("1-%09d", i), Status: control.StatusPending, Sequence: 1,
			Scope: &control.Scope{Resource: directory.PlacementResource("acme/data"), Clusters: []string{"c1"}}}
		if err := m.Create(b.Context(), op); err != nil {
			b.Fatal(err)
		}
		op.Status, op.Sequence = control.StatusRunning, 2
		if err := m.Update(b.Context(), op); err != nil {
			b.Fatal(err)
		}
		op.Status, op.Sequence = control.StatusSucceeded, 3
		if err := m.Update(b.Context(), op); err != nil {
			b.Fatal(err)
		}
	}
}
