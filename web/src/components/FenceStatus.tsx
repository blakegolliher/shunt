import type { Blocker } from '../api/client'
import { blockerText } from '../api/client'

// FenceStatus is where a change stands in the fleet (ADR-0016, ADR-0021 D2): pending on named
// proxies, blocked on named reasons, or applied. A blocked change is not a failure: it waits, and
// the operation can be cancelled before its commit.
export function FenceStatus({ held, waitingOn, version, phase, blockers }: { held: boolean; waitingOn: string[]; version: number; phase?: string; blockers?: Blocker[] }) {
  if (blockers && blockers.length > 0) {
    return <div role="status" className="rounded-lg border border-red-500/60 bg-red-950/30 p-3 text-sm text-paper">
      <span className="font-semibold text-red-200">Blocked</span>
      <span className="ml-2">{phase ? `${phase}: ` : ''}waiting on {blockerText(blockers)}</span>
      <ul className="mt-2 grid gap-1 text-xs text-red-100/90">{blockers.map((b) => b.message ? <li key={`${b.code}-${b.proxy_id ?? ''}-${b.incarnation ?? ''}`}>{b.message}</li> : null)}</ul>
    </div>
  }
  if (held || waitingOn.length > 0) {
    return <div role="status" className="rounded-lg border border-ember-500/50 bg-ember-500/10 p-3 text-sm text-paper">
      <span className="font-semibold text-ember-300">Fence pending</span>
      <span className="ml-2">{phase ? `${phase}: ` : ''}waiting on {waitingOn.length ? waitingOn.join(', ') : 'the held step'}</span>
    </div>
  }
  if (phase) {
    return <div role="status" className="rounded-lg border border-ember-500/50 bg-ember-500/10 p-3 text-sm text-paper">
      <span className="font-semibold text-ember-300">Operation pending</span>
      <span className="ml-2">{phase}</span>
    </div>
  }
  return <div role="status" className="rounded-lg border border-ink-700 bg-ink-800 p-3 text-sm text-muted">
    Applied at directory revision <span className="font-mono text-paper">{version}</span>
  </div>
}
