import { useEffect, useState } from 'react'
import { cancelOperation, listOperations, resolveWorker, resumeOperation, unfinished } from '../api/client'
import type { Operation } from '../api/client'
import { Card } from '../components/Card'
import { useStore } from '../store'

// Status and effect colors: unfinished work is amber, a failure or an uncertain effect is red.
const statusTone: Record<string, string> = {
  pending: 'border-amber-400/50 bg-amber-400/10 text-amber-200',
  running: 'border-amber-400/50 bg-amber-400/10 text-amber-200',
  blocked: 'border-red-500/60 bg-red-950/40 text-red-100',
  succeeded: 'border-ember-300/40 bg-ember-300/10 text-ember-300',
  failed: 'border-red-600/60 bg-red-950/40 text-red-100',
  cancelled: 'border-ink-700 bg-ink-800 text-muted',
}

function Badge({ text, tone }: { text: string; tone?: string }) {
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold tracking-wide ${tone ?? 'border-ink-700 bg-ink-800 text-muted'}`}>{text}</span>
}

// workerSession is the external mover session an operation's record names: its worker's, or, for
// a worker that never enrolled, the one its run was started with.
function workerSession(op: Operation): string {
  const fromArgs = op.args?.session
  return op.worker?.id ?? (typeof fromArgs === 'string' ? fromArgs : '')
}

function scopeOf(op: Operation): string {
  return op.scope?.resource ?? (op.placement ? `placement:${op.placement}` : op.cluster ? `cluster:${op.cluster}` : '—')
}

// Operations lists the records every change runs under (ADR-0021): what each reserves, where it is,
// and what it did to the directory. A record ends succeeded, failed or cancelled; failed never means
// rolled back, which is what the effect column says.
export function Operations() {
  const { token, lastEvent, notify } = useStore()
  const [ops, setOps] = useState<Operation[]>([])
  const [error, setError] = useState('')
  const [selected, setSelected] = useState('')
  const [busy, setBusy] = useState(false)
  const [attestation, setAttestation] = useState('')
  const fenceEvent = lastEvent?.type === 'fence' ? lastEvent.id : ''
  const act = async (fn: () => Promise<Operation>, done: (op: Operation) => string) => {
    setBusy(true)
    try { const op = await fn(); setOps((old) => old.map((o) => o.id === op.id ? op : o)); notify(done(op)) } catch (cause) { notify(cause instanceof Error ? cause.message : String(cause), 'danger') } finally { setBusy(false) }
  }

  useEffect(() => {
    let live = true
    const timer = window.setTimeout(() => {
      void listOperations(token).then((result) => { if (live) { setOps(result.operations); setError('') } })
        .catch((cause) => { if (live) setError(cause instanceof Error ? cause.message : String(cause)) })
    }, 0)
    return () => { live = false; window.clearTimeout(timer) }
  }, [fenceEvent, token])

  const current = ops.find((op) => op.id === selected)
  return <div className="grid gap-5 xl:grid-cols-[1fr_380px]">
    <Card eyebrow="Operation records" title="Recent operations">
      {error ? <p role="alert" className="rounded-lg border border-red-600 bg-red-950/40 p-3 text-sm text-red-100">{error}</p>
        : ops.length === 0 ? <p className="text-sm text-muted">No operations have run yet.</p>
          : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Updated</th><th className="pb-3">Kind</th><th className="pb-3">Scope</th><th className="pb-3">Status</th><th className="pb-3">Phase</th><th className="pb-3">Effect</th></tr></thead>
            <tbody className="divide-y divide-ink-700">{ops.map((op) => <tr key={op.id} aria-selected={op.id === selected} className={op.id === selected ? 'bg-ink-800' : ''}>
              <td className="py-3 text-muted">{op.updated ? new Date(op.updated).toLocaleTimeString() : '—'}</td>
              <td className="py-3"><button type="button" onClick={() => setSelected(op.id)} className="text-ember-300 underline-offset-2 hover:underline" aria-label={`Show operation ${op.id}`}>{op.kind}</button></td>
              <td className="py-3 font-mono text-xs">{scopeOf(op)}</td>
              <td className="py-3"><Badge text={op.status} tone={statusTone[op.status]} /></td>
              <td className="py-3 text-muted">{unfinished(op) ? op.phase ?? '—' : '—'}</td>
              <td className="py-3">{op.effect_state === 'uncertain' ? <Badge text="uncertain" tone={statusTone.failed} /> : <span className="text-muted">{op.effect_state ?? '—'}</span>}</td>
            </tr>)}</tbody>
          </table></div>}
    </Card>
    <Card eyebrow="Operation" title={current ? current.kind : 'Select an operation'}>
      {!current ? <p className="text-sm text-muted">Choose an operation to see what it reserves, where it is, and how it ended.</p>
        : <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-2 text-sm">
          <dt className="text-muted">ID</dt><dd className="font-mono text-xs">{current.id}</dd>
          <dt className="text-muted">Status</dt><dd><Badge text={current.status} tone={statusTone[current.status]} /></dd>
          <dt className="text-muted">Effect</dt><dd>{current.effect_state === 'uncertain' ? 'uncertain: its control node was lost while it could have been writing; repeat the step to complete it' : current.effect_state ?? '—'}</dd>
          <dt className="text-muted">Scope</dt><dd className="font-mono text-xs">{scopeOf(current)}{current.scope ? ` @ generation ${current.scope.generation}` : ''}</dd>
          {current.scope?.clusters && current.scope.clusters.length > 0 && <><dt className="text-muted">Touches</dt><dd className="font-mono text-xs">{current.scope.clusters.join(', ')}</dd></>}
          {current.phase && <><dt className="text-muted">Phase</dt><dd>{current.phase}</dd></>}
          {current.waiting_on && current.waiting_on.length > 0 && <><dt className="text-muted">Waiting on</dt><dd className="font-mono text-xs">{current.waiting_on.join(', ')}</dd></>}
          {current.blockers && current.blockers.length > 0 && <><dt className="text-muted">Blockers</dt><dd><ul>{current.blockers.map((b) => <li key={`${b.code}-${b.proxy_id ?? ''}`} className="font-mono text-xs">{b.code}{b.proxy_id ? ` ${b.proxy_id}` : ''}{b.message ? `: ${b.message}` : ''}</li>)}</ul></dd></>}
          {current.error && <><dt className="text-muted">Error</dt><dd role="alert" className="text-red-100">{current.error.code}: {current.error.message}</dd></>}
          {current.barrier && <><dt className="text-muted">Barrier</dt><dd className="text-xs">{current.barrier.kind} on <span className="font-mono">{current.barrier.scope}</span>{current.barrier.committed ? `, committed at version ${current.barrier.commit_version}` : current.barrier.hold_version ? `, held at version ${current.barrier.hold_version}, draining` : ', hold not written yet'}</dd></>}
          {current.worker && <><dt className="text-muted">Worker</dt><dd className="text-xs"><span className="font-mono">{current.worker.id}</span> {current.worker.state}, {current.worker.inflight ?? 0} in flight, {current.worker.uncertain ?? 0} uncertain{current.worker.resolved_by ? `; resolved by ${current.worker.resolved_by}: ${current.worker.attestation ?? ''}` : ''}</dd></>}
          <dt className="text-muted">Actions</dt><dd className="flex flex-wrap items-center gap-2 text-muted">{current.allowed_actions && current.allowed_actions.length > 0 ? current.allowed_actions.map((action) => action === 'resolve-worker'
            ? <form key={action} className="grid w-full gap-2" onSubmit={(event) => { event.preventDefault(); const session = workerSession(current); void act(() => resolveWorker(token, current.id, session, attestation.trim()), (op) => `worker session ${session} resolved; ${op.kind} ends and frees its bucket`).then(() => setAttestation('')) }}>
              <label htmlFor="worker-attestation" className="text-xs text-muted">The worker session expired with its work unresolved. Once its process and backend requests are known to have ended, say how:</label>
              <textarea id="worker-attestation" required value={attestation} onChange={(event) => setAttestation(event.target.value)} className="rounded-lg border border-ink-700 bg-ink-950 p-2 text-xs text-paper" rows={2} />
              <button type="submit" disabled={busy || attestation.trim() === '' || workerSession(current) === ''} className="justify-self-start rounded-lg border border-red-500 px-3 py-1 text-xs font-semibold text-red-100">Resolve worker session</button>
            </form>
            : <button key={action} type="button" disabled={busy} onClick={() => void act(() => action === 'resume' ? resumeOperation(token, current.id) : cancelOperation(token, current.id), (op) => action === 'resume' ? `${op.kind} resumed on ${op.node ?? 'this node'}` : `${op.kind} cancelled; nothing changed`)} className={`rounded-lg border px-3 py-1 text-xs font-semibold ${action === 'cancel' ? 'border-red-500 text-red-100' : 'border-ember-500 text-paper'}`}>{action === 'resume' ? 'Resume here' : 'Cancel before commit'}</button>) : 'none'}</dd>
          <dt className="text-muted">Actor</dt><dd className="font-mono text-xs">{current.actor}{current.node ? ` on ${current.node}` : ''}</dd>
          {current.request_id && <><dt className="text-muted">Request</dt><dd className="font-mono text-xs">{current.request_id}</dd></>}
        </dl>}
    </Card>
  </div>
}
