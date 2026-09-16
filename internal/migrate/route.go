package migrate

import (
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
//	delete               primary   both                       both             primary
//	list                 primary   merge                      merge            primary
//	bucket config        primary   primary                    primary          primary
func Decide(p *directory.Placement, class OpClass, key string) Route {
	// A placement with no second cluster has nothing to decide, and neither does a state that
	// keeps both sides in step.
	if p.Source == "" || p.State == directory.StateActive || p.State == directory.StateCutover {
		return Route{Cluster: Primary}
	}
	switch class {
	case ClassWrite:
		if p.State == directory.StateRamping && !InRange(p.Ramp, key) {
			return Route{Cluster: Source}
		}
		return Route{Cluster: Primary}
	case ClassRead:
		if p.State == directory.StateRamping && !InRange(p.Ramp, key) {
			// The target cannot hold a key whose writes still go to the source.
			return Route{Cluster: Source}
		}
		return Route{Cluster: Primary, Fallback: true}
	case ClassDelete:
		return Route{Cluster: Primary, Both: true}
	case ClassList:
		return Route{Cluster: Primary, Merge: true}
	}
	return Route{Cluster: Primary}
}

// InRange reports whether a key's writes have moved to the new primary. A key matches when it
// carries one of the ramp's prefixes, or when its hash falls under the ratio. The hash is FNV-1a
// over the key with no seed, so every proxy in a fleet decides identically and a restart does not
// move a key (docs/DESIGN.md §2.5).
func InRange(r *directory.Ramp, key string) bool {
	if r == nil {
		return false
	}
	for _, p := range r.Prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	switch {
	case r.Ratio <= 0:
		return false
	case r.Ratio >= 1:
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64() < uint64(r.Ratio*float64(math.MaxUint64))
}
