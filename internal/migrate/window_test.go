package migrate

import (
	"strings"
	"testing"
)

// The refusal is the operator's whole briefing: it has to name the cluster, the window, what to do,
// where to read more, and how to proceed anyway.
func TestRefuseLostWriteWindowSaysEverything(t *testing.T) {
	msg := RefuseLostWriteWindow("acme/data", "garage").Error()
	for _, want := range []string{"acme/data", "garage", "conditional_write: false", "If-None-Match", "HEAD-then-commit",
		"overwritten", "ADR-0004 race 2", "Quiesce writers", "docs/migrating.md", "--accept-lost-write-window"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal does not mention %q: %s", want, msg)
		}
	}
	if !strings.HasPrefix(msg, LostWriteWindow("acme/data", "garage")) {
		t.Error("the refusal and the warning must say the same thing")
	}
}

func BenchmarkRefuseLostWriteWindow(b *testing.B) {
	for b.Loop() {
		_ = RefuseLostWriteWindow("acme/data", "garage")
	}
}
