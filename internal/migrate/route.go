package migrate

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
)

// OpClass is the row of the routing table in docs/DESIGN.md §2.5 that an operation belongs to.
// Every operation has one; a new operation without one fails TestEveryOpHasAClass.
type OpClass uint8

// The classes of §2.5, plus the two shunt answers itself.
const (
	ClassRead    OpClass = iota + 1 // GET, HEAD, and the per-object metadata reads
	ClassWrite                      // PUT, POST, copy, and the per-object metadata writes
	ClassDelete                     // object deletes: they go to both clusters mid-migration
	ClassList                       // object listings: merged mid-migration
	ClassBucket                     // bucket-level configuration: always the primary
	ClassUpload                     // carries an uploadId: pinned to the cluster that issued it
	ClassService                    // ListBuckets: shunt answers from the directory
)

// Class maps an operation to its routing class.
func Class(op s3.Op) OpClass {
	switch op {
	// The multipart operations that carry an uploadId are pinned by the codec (§2.4), never by
	// the ramp: the upload only exists on the cluster that issued its id.
	case s3.OpUploadPart, s3.OpUploadPartCopy, s3.OpCompleteMultipartUpload, s3.OpAbortMultipartUpload, s3.OpListParts:
		return ClassUpload

	case s3.OpGetObject, s3.OpHeadObject, s3.OpGetObjectAcl, s3.OpGetObjectAttributes,
		s3.OpGetObjectTagging, s3.OpGetObjectLegalHold, s3.OpGetObjectRetention,
		s3.OpGetObjectTorrent, s3.OpSelectObjectContent:
		return ClassRead

	case s3.OpPutObject, s3.OpCopyObject, s3.OpPostObject, s3.OpCreateMultipartUpload,
		s3.OpPutObjectAcl, s3.OpPutObjectTagging, s3.OpPutObjectLegalHold, s3.OpPutObjectRetention,
		s3.OpRestoreObject:
		return ClassWrite

	case s3.OpDeleteObject, s3.OpDeleteObjects, s3.OpDeleteObjectTagging:
		return ClassDelete

	case s3.OpListObjects, s3.OpListObjectsV2, s3.OpListObjectVersions, s3.OpListMultipartUploads:
		return ClassList

	case s3.OpListBuckets:
		return ClassService
	}
	// Everything else is bucket-level configuration, including HeadBucket, CreateBucket,
	// DeleteBucket, and every ?subresource. Unknown and Preflight ride along: they are proxied
	// to the primary, which is what they did before any migration started.
	return ClassBucket
}

// Route is where one request goes. Exactly one of Primary or Source is the first attempt;
// Fallback and Both describe what happens after it.
type Route struct {
	Cluster  Side // which side the request goes to first
	Fallback bool // read: on 404 from Cluster, try the other side
	Both     bool // delete: send to both sides
	Merge    bool // list: merge both sides into one answer
	// Held: a write to a key inside a ramp step that has not reached every proxy (ADR-0016). It is
	// refused with 503 and Retry-After rather than sent anywhere.
	Held bool
}

// Side names one of a placement's two clusters.
type Side uint8

// The two sides.
const (
	Primary Side = iota + 1
	Source
)

func (s Side) String() string {
	if s == Source {
		return "source"
	}
	return "primary"
}

// Decide applies the §2.5 table. key is the object key, used only by the ramp rule; it is
// ignored for classes whose routing does not depend on it.
//
//	                     ACTIVE    RAMPING (in range / out)   MIGRATING        CUTOVER
//	write                primary   primary / source           primary          primary
//	read                 primary   primary→source / source    primary→source   primary
//	delete               primary   both                       both             both
//	list                 primary   merge                      merge            primary
//	bucket config        primary   primary                    primary          primary
//
// A delete goes to both clusters through CUTOVER, not just MIGRATING, so the source stays a subset
// of the primary until purge-source compares them (ADR-0004 amendment, POC-5).
func Decide(p *directory.Placement, class OpClass, key string) (Route, error) {
	// A placement with no second cluster has nothing to decide, and neither does ACTIVE.
	if p.Source == "" || p.State == directory.StateActive {
		return Route{Cluster: Primary}, nil
	}
	if p.State == directory.StateCutover {
		if class == ClassDelete {
			return Route{Cluster: Primary, Both: true}, nil
		}
		return Route{Cluster: Primary}, nil
	}
	switch class {
	case ClassWrite, ClassRead:
		if p.State == directory.StateRamping {
			in, err := InRange(p.Ramp, key)
			if err != nil {
				return Route{}, err
			}
			if !in {
				held, err := InHold(p.Ramp, key)
				if err != nil {
					return Route{}, err
				}
				switch {
				case held && class == ClassWrite:
					// No proxy writes a held key to the target until every proxy holds it (ADR-0016).
					return Route{Cluster: Primary, Held: true}, nil
				case held:
					// Another proxy may already have the completed step and have written the key there.
					return Route{Cluster: Primary, Fallback: true}, nil
				}
				// A key whose writes still go to the source is read there too: the target cannot have it.
				return Route{Cluster: Source}, nil
			}
		}
		if class == ClassRead {
			return Route{Cluster: Primary, Fallback: true}, nil
		}
		return Route{Cluster: Primary}, nil
	case ClassDelete:
		return Route{Cluster: Primary, Both: true}, nil
	case ClassList:
		return Route{Cluster: Primary, Merge: true}, nil
	}
	return Route{Cluster: Primary}, nil
}

// ErrUnknownRampHash is returned for a ramp whose keys are split by a hash this build does not
// implement. Routing such a ramp with a different hash would move keys between sides mid-ramp and
// serve stale reads (ADR-0004 race 5), so the request is refused instead.
var ErrUnknownRampHash = errors.New("unknown ramp hash")

// InRange reports whether a key's writes have moved to the new primary. A key matches when it
// carries one of the ramp's prefixes, or when its hash falls under the ratio. The hash is the one
// the ramp names (directory.Ramp.Hash); this build implements directory.RampHash, FNV-1a over the
// key with no seed, so every proxy in a fleet decides identically and a restart does not move a
// key (docs/DESIGN.md §2.5), finished with murmur3's 64-bit mixer: FNV-1a alone barely moves its
// high bits for keys that differ only in their last bytes, so sequential keys (a/0000 … a/0999)
// all fell on one side of a ratio of 0.5 (found by the POC-5 walkthrough; ADR-0004 amendment).
// A ramp naming another hash is refused with ErrUnknownRampHash wherever the hash would decide:
// not for a key a prefix already moved, and not at a ratio of 0 or 1.
func InRange(r *directory.Ramp, key string) (bool, error) {
	if r == nil {
		return false, nil
	}
	if r.Range != nil {
		return inRangeOf(r, key)
	}
	for _, p := range r.Prefixes {
		if strings.HasPrefix(key, p) {
			return true, nil
		}
	}
	if r.Ratio <= 0 {
		return false, nil
	}
	if r.Ratio >= 1 {
		return true, nil // every key, whatever the hash
	}
	if r.Hash != directory.RampHash {
		return false, fmt.Errorf("%w %q: this build splits keys by %s", ErrUnknownRampHash, r.Hash, directory.RampHash)
	}
	return rampHash(key) < uint64(r.Ratio*float64(math.MaxUint64)), nil
}

// inRangeOf is InRange for a ramp limited to a hash range (ADR-0018 N3): a key outside the range
// is never moved; inside it, a prefix moves it, and the ratio is a share of the range, counted from
// its start. The range always needs the hash, so a ramp naming another one is always refused.
func inRangeOf(r *directory.Ramp, key string) (bool, error) {
	if r.Hash != directory.RampHash {
		return false, fmt.Errorf("%w %q: this build splits keys by %s", ErrUnknownRampHash, r.Hash, directory.RampHash)
	}
	h := rampHash(key)
	from, to := uint64(r.Range.From), uint64(r.Range.To)
	if h < from || h > to {
		return false, nil
	}
	for _, p := range r.Prefixes {
		if strings.HasPrefix(key, p) {
			return true, nil
		}
	}
	switch {
	case r.Ratio <= 0:
		return false, nil
	case r.Ratio >= 1:
		return true, nil
	}
	return float64(h-from) < r.Ratio*(float64(to-from)+1), nil
}

// InRangeHash reports whether key's hash falls in rg, by the hash the placement names.
func InRangeHash(rg directory.HashRange, key string) bool {
	h := directory.Hash(rampHash(key))
	return h >= rg.From && h <= rg.To
}

// InHold reports whether a key falls inside the ramp's held step (directory.Ramp.Hold), split by
// the same hash. A key already in the ramp is in force, not held; callers ask InRange first.
func InHold(r *directory.Ramp, key string) (bool, error) {
	if r == nil || r.Hold == nil {
		return false, nil
	}
	return InRange(&directory.Ramp{Hash: r.Hash, Ratio: r.Hold.Ratio, Prefixes: r.Hold.Prefixes, Range: r.Range}, key)
}

// OwnerOf returns the id of the leg that owns key in a placement spread over legs (ADR-0018 N2):
// in the table of the key's scope (the longest prefix rule it starts with, ADR-0020), the leg
// whose hash range holds the key's hash, by the placement's own hash. A placement naming a hash
// this build does not implement is refused with ErrUnknownRampHash, never split differently.
func OwnerOf(p *directory.Placement, key string) (string, error) {
	if p.KeyHash != directory.RampHash {
		return "", fmt.Errorf("%w %q: this build splits keys by %s", ErrUnknownRampHash, p.KeyHash, directory.RampHash)
	}
	h := directory.Hash(rampHash(key))
	_, owners := p.Scope(key)
	leg := directory.OwnerIn(owners, h)
	if leg == "" {
		return "", fmt.Errorf("owners do not cover hash %016x", uint64(h)) // validation makes this unreachable
	}
	return leg, nil
}

// InMove reports whether key is one the placement's move is taking to another leg: in the move's
// scope and hash range. It is the one test of move membership: the proxy's narrowing, the listing
// merge, the mover and purge all ask it (ADR-0020), so a key has the same two homes everywhere.
func InMove(p *directory.Placement, key string) bool {
	m := p.Move
	if m == nil || !InRangeHash(m.Range, key) {
		return false
	}
	scope, _ := p.Scope(key)
	return scope == m.Scope
}

// Narrow is a spread placement as one request for key sees it: ACTIVE on the leg that owns the key,
// a plain one-cluster placement that every per-object path routes as it always has; or, for a key
// in the range a move is taking to another leg, the move's two-cluster migration (MoveView), which
// every per-object migration path routes as it always has. At rest a leg is the only home of its
// keys, so there is no fallback and nothing to merge.
func Narrow(p *directory.Placement, key string) (directory.Placement, error) {
	if m := p.Move; m != nil {
		if p.KeyHash != directory.RampHash {
			return directory.Placement{}, fmt.Errorf("%w %q: this build splits keys by %s", ErrUnknownRampHash, p.KeyHash, directory.RampHash)
		}
		if InMove(p, key) {
			// A key in the moving range: the move is the two-cluster migration it is (ADR-0018 N3).
			return p.MoveView(), nil
		}
	}
	id, err := OwnerOf(p, key)
	if err != nil {
		return directory.Placement{}, err
	}
	return onLeg(p, id), nil
}

// FirstLeg is a spread placement narrowed to the leg owning the start of the key space: where a
// bucket-level request that every leg answers alike (HeadBucket, GetBucketLocation) goes.
func FirstLeg(p *directory.Placement) directory.Placement { return onLeg(p, p.Owners[0].Leg) }

func onLeg(p *directory.Placement, id string) directory.Placement {
	l := p.Legs[id]
	return directory.Placement{State: directory.StateActive, Primary: l.Cluster, Names: map[string]string{l.Cluster: l.Bucket},
		Created: p.Created, ReadOnly: p.ReadOnly, RejectWrites: p.RejectWrites}
}

// rampHash is directory.RampHash, fnv1a-fmix64-v1. Its values are pinned by a test: changing them
// needs a new name.
func rampHash(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return mix64(h.Sum64())
}

// mix64 is murmur3's fmix64 finalizer: every input bit reaches every output bit.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
