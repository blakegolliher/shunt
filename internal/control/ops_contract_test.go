package control_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/control/opstest"
	"github.com/blakegolliher/shunt/internal/directory"
)

// The lab's in-memory store passes the operation transaction contract over a directory file.
func TestMemOperationsContract(t *testing.T) {
	opstest.Run(t, func(t *testing.T, limit int) opstest.Harness {
		path := filepath.Join(t.TempDir(), "directory.yaml")
		if err := os.WriteFile(path, []byte("version: 0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		d, err := directory.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return opstest.Harness{Ops: &control.MemOperations{Dir: d, Limit: limit}, Dir: d}
	})
}
