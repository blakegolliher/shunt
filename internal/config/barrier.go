package config

import "fmt"

// Barrier is a drain barrier written on a placement or a cluster together with the change it
// guards (ADR-0021 D2). Every proxy that installs the directory closes the gate the barrier names
// and reports, in each heartbeat, that its work through that gate has drained; the control node
// commits the change only once every proxy has. ID is the operation's; Kind names the gate.
type Barrier struct {
	ID   string `yaml:"id" json:"id"`
	Kind string `yaml:"kind" json:"kind"`
}

// The gates a barrier closes.
const (
	// BarrierMutations closes every request that can change the placement's backend buckets:
	// writes, deletes, multipart parts and completion, bucket configuration, bucket deletion.
	BarrierMutations = "mutations"
	// BarrierSource closes work that depends on a moving placement's source: reads falling back
	// to it, listings merging it, copies reading it, the source leg of a dual delete. Only a
	// placement in CUTOVER carries one: purge-source writes it before it deletes the source.
	BarrierSource = "source"
)

// Validate checks the id and the kind.
func (b *Barrier) Validate() error {
	if !ValidBarrierID(b.ID) {
		return fmt.Errorf("barrier id %q: want 1-64 letters, digits, '.', '_', ':' or '-'", b.ID)
	}
	if b.Kind != BarrierMutations && b.Kind != BarrierSource {
		return fmt.Errorf("barrier kind %q: want %s or %s", b.Kind, BarrierMutations, BarrierSource)
	}
	return nil
}

// ValidBarrierID reports whether id can name a barrier: an operation id, or a test's.
func ValidBarrierID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != ':' && c != '-' {
			return false
		}
	}
	return true
}
