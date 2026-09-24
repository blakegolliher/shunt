package proxy

import (
	"errors"
	"sync"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// GateKeeper keeps a proxy's admission gates in step with its directory (ADR-0021 D2). A gate is
// closed when the installed directory version or the one requests route by (the published runtime
// bundle's) carries a barrier for it, and it is never opened while requests still route by an
// older version than the installed one: a gate opened for a version no request uses yet would let
// a request on the old version write by the old rule. Both the member and the lab proxy drive it
// from their install and publish hooks.
type GateKeeper struct {
	Gates *admission.Gates

	mu                sync.Mutex
	installed, served *directory.Snapshot
}

// Installed records a newly installed directory version and applies its barriers.
func (k *GateKeeper) Installed(s *directory.Snapshot) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.installed = s
	k.applyLocked()
}

// Served records the directory version new requests route by and applies its barriers.
func (k *GateKeeper) Served(s *directory.Snapshot) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.served = s
	k.applyLocked()
}

func (k *GateKeeper) applyLocked() {
	closures := admission.Closures{}
	for _, snap := range []*directory.Snapshot{k.installed, k.served} {
		if snap == nil {
			continue
		}
		c, _ := admission.Barriers(snap)
		for key, closed := range c {
			have := closures[key]
			for i := range have {
				if have[i] == "" {
					have[i] = closed[i]
				}
			}
			closures[key] = have
		}
	}
	sticky := k.installed != nil && k.served != nil && k.served.Version() < k.installed.Version()
	k.Gates.Apply(closures, sticky)
}

// dispatched reports whether a failed upstream exchange may have reached the backend whole: the
// request was not refused at connect, was built, and its body, if any, was sent to the end. Such
// a failure leaves the backend's outcome unknown (admission.Uncertain): the backend may have
// committed the change after this proxy stopped listening. A body cut short before the end never
// made a complete request, so the backend could not have committed it.
func dispatched(err error, inBody *progressReader, contentLength int64) bool {
	var be *buildError
	if err == nil || upstream.IsConnectError(err) || errors.As(err, &be) {
		return false
	}
	if inBody == nil {
		return true
	}
	return contentLength < 0 || inBody.count() >= contentLength
}

// releaseTokens gives back the admission tokens a request took, with how its backend work ended.
func (o *outcome) releaseTokens(g *admission.Gates) {
	mut, src := admission.Definitive, admission.Definitive
	if o.uncertain {
		mut = admission.Uncertain
	}
	if o.srcUncertain {
		src = admission.Uncertain
	}
	o.tok.Release(g, mut)
	o.src.Release(g, src)
}
