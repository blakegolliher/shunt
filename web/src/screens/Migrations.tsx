import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { fullRange, leadingShare } from '../hashRange'
import {
  getMoverLedger,
  getOperation,
  getPlacementView,
  purgeSourceDryRun,
  removeCluster,
  removeClusterDryRun,
  setTenantDefault,
  startOperation,
  unfinished,
} from '../api/client'
import type { LedgerEntry, Operation, PlacementStatus, PlacementView, PurgeDryRun, RemoveDryRun } from '../api/client'
import { Card } from '../components/Card'
import { ConfirmDrawer } from '../components/ConfirmDrawer'
import { FenceStatus } from '../components/FenceStatus'
import { Sparkline } from '../components/Sparkline'
import { StateBadge } from '../components/StateBadge'
import { OwnershipBar } from '../components/OwnershipBar'
import { useStore } from '../store'

const inputClass = 'mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper'

function splitKey(key: string): [string, string] {
  const at = key.indexOf('/')
  return [key.slice(0, at), key.slice(at + 1)]
}

function humanBytes(value: number) {
  if (value >= 1 << 30) return `${(value / (1 << 30)).toFixed(1)} GiB`
  if (value >= 1 << 20) return `${(value / (1 << 20)).toFixed(1)} MiB`
  if (value >= 1 << 10) return `${(value / (1 << 10)).toFixed(1)} KiB`
  return `${value} B`
}

function SplitBar({ left, right, leftLabel, rightLabel }: { left: number; right: number; leftLabel: string; rightLabel: string }) {
  const total = left + right
  const leftWidth = total > 0 ? (left / total) * 100 : 0
  const rightWidth = total > 0 ? 100 - leftWidth : 0
  return <div>
    <div className="mb-2 flex justify-between text-xs text-muted"><span>{leftLabel} {left.toLocaleString()}</span><span>{rightLabel} {right.toLocaleString()}</span></div>
    <div className="flex h-3 overflow-hidden rounded-full bg-ink-700" aria-label={`${leftLabel} ${left}; ${rightLabel} ${right}`}>
      <span className="bg-ember-700" style={{ width: `${leftWidth}%` }} />
      <span className="bg-ember-400" style={{ width: `${rightWidth}%` }} />
    </div>
  </div>
}

function ReadBar({ hit, fallback, miss }: { hit: number; fallback: number; miss: number }) {
  const total = hit + fallback + miss
  const width = (value: number) => total > 0 ? `${value / total * 100}%` : '0%'
  return <div>
    <div className="mb-2 flex flex-wrap justify-between gap-2 text-xs text-muted"><span>Target hit {hit.toLocaleString()}</span><span>Fallback to source {fallback.toLocaleString()}</span><span>Miss {miss.toLocaleString()}</span></div>
    <div className="flex h-3 overflow-hidden rounded-full bg-ink-700" aria-label={`target hit ${hit}; fallback to source ${fallback}; miss ${miss}`}>
      <span className="bg-emerald-500" style={{ width: width(hit) }} />
      <span className="bg-amber-500" style={{ width: width(fallback) }} />
      <span className="bg-red-500" style={{ width: width(miss) }} />
    </div>
  </div>
}

// appliedRatio is the share of keys whose writes go to the target: the ramp while RAMPING, every
// key once MIGRATING or CUTOVER (the ramp is gone then, not zero), none before the first step.
function appliedRatio(view: { state: string; ratio?: number }) {
  if (view.state === 'MIGRATING' || view.state === 'CUTOVER') return 1
  return view.state === 'RAMPING' ? view.ratio ?? 0 : 0
}

// A bucket that is not migrating, and what would start one: Expand for a plain bucket, Move keys or
// Consolidate for one spread over legs.
function StartMigration({ placements, onPrepare }: { placements: PlacementStatus[]; onPrepare: (key: string, action: 'expand' | 'move' | 'consolidate') => void }) {
  if (placements.length === 0) return <p className="text-sm text-muted">No other buckets.</p>
  return <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Bucket</th><th className="pb-3">State</th><th className="pb-3">Where</th><th className="pb-3 text-right">Start</th></tr></thead>
    <tbody className="divide-y divide-ink-700">{placements.map((p) => {
      const spread = (p.legs?.length ?? 0) > 1
      return <tr key={p.key}><td className="py-3 font-semibold text-ember-300">{p.key}</td><td className="py-3"><StateBadge state={p.state} /></td>
        <td className="py-3 text-muted">{spread ? `spread: ${p.legs?.map((l) => l.cluster).join(' + ')}` : p.primary}</td>
        <td className="py-3 text-right">{p.state !== 'ACTIVE' ? <span className="text-xs text-muted">busy</span> : spread
          ? <><button type="button" aria-label={`Move keys of ${p.key}`} onClick={() => onPrepare(p.key, 'move')} className="mr-2 rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold">Move keys</button><button type="button" aria-label={`Consolidate ${p.key}`} onClick={() => onPrepare(p.key, 'consolidate')} className="rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold">Consolidate</button></>
          : <button type="button" aria-label={`Expand ${p.key}`} onClick={() => onPrepare(p.key, 'expand')} className="rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold">Expand to another cluster</button>}</td></tr>
    })}</tbody></table></div>
}

export function Migrations({ selected, onSelect, onPrepare }: { selected: string; onSelect: (key: string) => void; onPrepare: (key: string, action: 'expand' | 'move' | 'consolidate') => void }) {
  const { token, directory, fleet, lastEvent, notify, refresh } = useStore()
  const candidates = useMemo(() => (directory?.placements ?? []).filter((item) => item.target || item.source || item.state !== 'ACTIVE'), [directory])
  const key = selected || candidates[0]?.key || ''
  const [tenant] = splitKey(key || '/')
  const [detail, setDetail] = useState<PlacementView | null>(null)
  const [operation, setOperation] = useState<Operation | null>(null)
  const [ratio, setRatio] = useState(0.5)
  const [prefixes, setPrefixes] = useState('')
  const [moveShare, setMoveShare] = useState(1) // the first step: every key (1), or a leading share of the key space
  const [acceptLoss, setAcceptLoss] = useState(false)
  const [cutoverWindow, setCutoverWindow] = useState('5s')
  const [purge, setPurge] = useState<PurgeDryRun | null>(null)
  const [remove, setRemove] = useState<RemoveDryRun | null>(null)
  const [ledger, setLedger] = useState<LedgerEntry[]>([])
  const [formerSource, setFormerSource] = useState('')
  const [fallbackTrend, setFallbackTrend] = useState<{ key: string; end: string; value: number }[]>([])
  const [busy, setBusy] = useState(false)
  const synced = useRef<{ key: string; ratio: number; prefixes: string } | null>(null)

  const load = useCallback(async () => {
    if (!key) { setDetail(null); return }
    const [tenant, bucket] = splitKey(key)
    try {
      const view = await getPlacementView(token, tenant, bucket)
      setDetail(view)
      setFallbackTrend((old) => {
        const sameBucket = old.length > 0 && old[old.length - 1].key === key ? old : []
        const window = view.migration_window
        if (!window || sameBucket[sameBucket.length - 1]?.end === window.end) return sameBucket
        return [...sameBucket.slice(-11), { key, end: window.end, value: window.reads.fallback_source ?? 0 }]
      })
      if (view.source) setFormerSource(view.source)
      // The slider is the operator's draft. Every live update reloads the view, so it follows the
      // server only when the applied ramp itself changes (or the bucket does), never on a reload.
      const applied = appliedRatio(view)
      const appliedPrefixes = (view.prefixes ?? []).join('\n')
      const last = synced.current
      if (!last || last.key !== key || last.ratio !== applied || last.prefixes !== appliedPrefixes) {
        synced.current = { key, ratio: applied, prefixes: appliedPrefixes }
        setRatio(Math.max(applied, 0.01))
        setPrefixes(appliedPrefixes)
      }
      if (view.mover) {
        try { setLedger((await getMoverLedger(token, tenant, bucket)).entries) } catch { setLedger([]) }
      }
    } catch (error) {
      notify(error instanceof Error ? error.message : String(error), 'danger')
    }
  }, [key, notify, token])

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), 0)
    return () => window.clearTimeout(timer)
  }, [load, directory?.version, lastEvent?.id])

  useEffect(() => {
    if (!operation || !unfinished(operation)) return
    let stopped = false
    const poll = async () => {
      try {
        const next = await getOperation(token, operation.id)
        if (stopped) return
        setOperation(next)
        if (unfinished(next)) window.setTimeout(() => void poll(), 350)
        else {
          notify(next.error?.message ?? `${next.kind} ${next.status}`, next.status === 'succeeded' ? 'success' : 'danger')
          await refresh()
          await load()
        }
      } catch (error) {
        if (!stopped) notify(error instanceof Error ? error.message : String(error), 'danger')
      }
    }
    const timer = window.setTimeout(() => void poll(), 350)
    return () => { stopped = true; window.clearTimeout(timer) }
  }, [load, notify, operation, refresh, token])

  const run = async (kind: string, args?: unknown) => {
    if (!key) return false
    setBusy(true)
    try {
      const next = await startOperation(token, kind, key, args)
      setOperation(next)
      notify(`${kind} started`)
      return true
    } catch (error) {
      notify(error instanceof Error ? error.message : String(error), 'danger')
      return false
    } finally { setBusy(false) }
  }

  const others = (directory?.placements ?? []).filter((item) => !candidates.some((c) => c.key === item.key))
  if (!key) return <Card eyebrow="Migration workflow" title="No bucket is migrating"><p className="mb-4 text-sm text-muted">A migration starts from a bucket: expand a plain bucket to another cluster, or move keys of a bucket spread over legs. Pick one to begin.</p><StartMigration placements={others} onPrepare={onPrepare} /></Card>
  if (!detail) return <Card title={key}><p className="text-muted">Loading migration state…</p></Card>

  const source = detail.source || (detail.target ? detail.primary : formerSource)
  const target = detail.target || (detail.source || formerSource ? detail.primary : '')
  const currentRatio = appliedRatio(detail)
  const writeSource = detail.migration_window?.writes.source ?? detail.ramp_writes?.source ?? 0
  const writeTarget = detail.migration_window?.writes.primary ?? detail.ramp_writes?.primary ?? 0
  const targetHits = detail.migration_window?.reads.target_hit ?? 0
  const fallbackReads = detail.migration_window?.reads.fallback_source ?? 0
  const readMisses = detail.migration_window?.reads.miss ?? 0
  const stale = (fleet?.members ?? []).filter((member) => !member.live).map((member) => member.id)
  const opWaiting = operation && unfinished(operation) ? operation.waiting_on ?? [] : detail.fence.waiting_on
  const prefixesList = prefixes.split('\n').map((value) => value.trim()).filter(Boolean)
  const targetProfile = target ? detail.clusters[target] : undefined
  const assumedTarget = Boolean(targetProfile && targetProfile.conditional_write_known !== true)
  const unsafeTarget = targetProfile?.conditional_write === false
  const moverNeedsAcceptance = assumedTarget || unsafeTarget
  const moverReady = detail.state === 'MIGRATING' || detail.state === 'RAMPING' && currentRatio >= 1
  const formerSourceStatus = directory?.clusters.find((cluster) => cluster.name === formerSource)
  const sourceIsTenantDefault = Boolean(formerSourceStatus?.references?.includes(`tenants.${tenant}.default_cluster`))
  const canRemoveFormerSource = detail.state === 'ACTIVE' && Boolean(formerSource) && (formerSourceStatus?.references?.length ?? 0) === 0
  const mover = detail.mover
  const progress = operation?.progress

  const firstStep = detail.state === 'ACTIVE' && !detail.legs?.length
  const applyRamp = () => run('ramp', { ratio, prefixes: prefixesList, wait: '30s', ...(firstStep && moveShare < 1 ? { range: leadingShare(fullRange, moveShare) } : {}) })
  const dryRunPurge = async () => {
    const [tenant, bucket] = splitKey(key)
    setBusy(true)
    try {
      const plan = await purgeSourceDryRun(token, tenant, bucket)
      setPurge(plan)
      if (!plan.allowed && plan.reason) notify(plan.reason, 'danger')
    } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger') } finally { setBusy(false) }
  }
  const dryRunRemove = async () => {
    if (!formerSource) return
    setBusy(true)
    try {
      const plan = await removeClusterDryRun(token, formerSource)
      setRemove(plan)
      if (!plan.allowed && plan.reason) notify(plan.reason, 'danger')
    } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger') } finally { setBusy(false) }
  }

  return <div className="space-y-5">
    {stale.length > 0 && <div role="alert" className="rounded-xl border border-red-600 bg-red-950/50 p-4 text-sm text-red-100"><span className="font-semibold">Stale {stale.join(', ')}</span>: writes on this moving bucket answer 503 from {stale.length === 1 ? 'that proxy' : 'those proxies'} until they reconnect and apply the current revision.</div>}

    <Card eyebrow="Migration" title={`${source || 'source'} → ${target || 'target'}`} action={<StateBadge state={detail.state} />}>
      <label className="mb-4 block text-sm text-muted">Bucket<select value={key} onChange={(event) => onSelect(event.target.value)} className={inputClass}>{candidates.map((item) => <option key={item.key} value={item.key}>{item.key}</option>)}</select></label>
      {detail.move && <div className="mb-3 rounded-lg bg-ink-950 p-3 text-sm"><p>Moving <span className="font-semibold text-ember-300">{Math.round(detail.move.share * 1000) / 10}%</span> of the bucket's keys, leg {detail.move.from} → leg {detail.move.to}; the rest of the bucket stays where it is.</p>{detail.legs?.length ? <div className="mt-3"><OwnershipBar legs={detail.legs} move={detail.move} /></div> : null}</div>}
      <FenceStatus held={detail.fence.held || operation?.phase === 'hold'} version={operation?.version ?? detail.fence.version} waitingOn={opWaiting} phase={operation && unfinished(operation) ? operation.phase : undefined} />
      {operation && <div className="mt-3 rounded-lg bg-ink-950 p-3 text-xs"><span className="font-mono text-ember-300">{operation.id}</span><span className="ml-2 text-muted">{operation.kind} · {operation.status} · {operation.phase ?? 'done'}</span>{operation.error && <p className="mt-2 text-red-200">{operation.error.message}</p>}</div>}
    </Card>

    <Card eyebrow="Step 5–6" title="Ramp traffic">
      <div className="grid gap-5 lg:grid-cols-[1fr_1fr]">
        <div>
          <div className="flex flex-wrap gap-2">{[0.01, 0.25, 0.5, 1].map((preset) => <button key={preset} type="button" disabled={preset < currentRatio} onClick={() => setRatio(preset)} className={`rounded-full border px-3 py-1.5 text-xs font-semibold disabled:opacity-30 ${ratio === preset ? 'border-ember-400 bg-ember-600 text-white' : 'border-ink-700 text-muted'}`}>{preset * 100}%</button>)}</div>
          <label className="mt-4 block text-sm font-medium">Traffic ratio: {Math.round(ratio * 100)}% to target <span className="text-muted">(applied {Math.round(currentRatio * 100)}%)</span><input aria-label="Traffic ratio" type="range" min="0" max="1" step="0.01" value={ratio} onChange={(event) => setRatio(Math.max(Number(event.target.value), currentRatio, 0.01))} className="mt-3 w-full accent-ember-500" /><span aria-hidden="true" className="relative mt-1 block h-3"><span className="absolute top-0 h-3 border-l border-muted" style={{ left: `${currentRatio * 100}%` }} /><span className="absolute left-0 top-0 h-1 rounded bg-ember-600/40" style={{ width: `${currentRatio * 100}%` }} /></span></label>
          {firstStep && <fieldset className="mt-4 rounded-lg border border-ink-700 p-3"><legend className="px-2 text-sm font-medium">Keys to move</legend><div className="flex flex-wrap items-center gap-4 text-sm"><label className="flex items-center gap-2"><input type="radio" checked={moveShare >= 1} onChange={() => setMoveShare(1)} />All keys</label><label className="flex items-center gap-2"><input type="radio" checked={moveShare < 1} onChange={() => setMoveShare(0.5)} />Part of the bucket</label></div>{moveShare < 1 && <label className="mt-3 block text-sm">{Math.round(moveShare * 100)}% of the key space, chosen by key hash; the rest stays on {source}<input aria-label="Share of keys to move" type="range" min="0.01" max="0.99" step="0.01" value={moveShare} onChange={(event) => setMoveShare(Number(event.target.value))} className="mt-2 w-full accent-ember-500" /></label>}</fieldset>}
          <label className="mt-4 block text-sm font-medium">Prefix rules <span className="text-muted">(one per line)</span><textarea value={prefixes} onChange={(event) => setPrefixes(event.target.value)} rows={3} className={inputClass} placeholder="runs/2026-09/" /></label>
          <button type="button" disabled={busy || ratio < currentRatio || !target || unfinished(operation) || (detail.state !== 'ACTIVE' && detail.state !== 'RAMPING')} onClick={() => void applyRamp()} className="mt-4 rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50">Apply ramp</button>
          {detail.state === 'RAMPING' && currentRatio >= 1 && <button type="button" disabled={busy || unfinished(operation)} onClick={() => void run('migrate', { accept_lost_write_window: acceptLoss, wait: '30s' })} className="ml-3 mt-4 rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold">Enter MIGRATING</button>}
        </div>
        <div className="space-y-5 rounded-lg bg-ink-950 p-4">
          <div><p className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted">Live write split · last 10 s window</p><SplitBar left={writeSource} right={writeTarget} leftLabel={source || 'source'} rightLabel={target || 'target'} /></div>
          <div><p className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted">Read outcomes · last 10 s window</p><ReadBar hit={targetHits} fallback={fallbackReads} miss={readMisses} /></div>
        </div>
      </div>
    </Card>

    <Card eyebrow="Step 6" title="Mover">
      {moverNeedsAcceptance && <div className="mb-4 rounded-lg border border-red-600/60 bg-red-950/40 p-4 text-sm text-red-100"><p className="font-semibold">{assumedTarget ? 'The target’s conditional-write profile is assumed.' : 'The target does not honor conditional PUT.'}</p><p className="mt-2">ADR-0004: a client write between the mover's HEAD and PUT can be overwritten with older source bytes. Measure the profile or quiesce writers; starting anyway explicitly accepts this lost-write window.</p><label className="mt-3 flex items-center gap-2"><input type="checkbox" checked={acceptLoss} onChange={(event) => setAcceptLoss(event.target.checked)} /> I accept the lost-write window</label></div>}
      <button type="button" disabled={busy || !moverReady || moverNeedsAcceptance && !acceptLoss || unfinished(operation)} onClick={() => void run('mover', { until_converged: true, max_passes: 10, accept_lost_write_window: acceptLoss })} className="rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50">Start mover</button>
      {mover ? <div className="mt-5 space-y-4">
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">{[['Copied', mover.copied], ['Already there', mover.skipped], ['Vanished', mover.vanished], ['Failed', mover.failed], ['Moved', humanBytes(mover.bytes)]].map(([label, value]) => <div key={String(label)} className="rounded-lg bg-ink-950 p-3"><p className="text-xs text-muted">{label}</p><p className="mt-1 font-mono text-lg">{value}</p></div>)}</div>
        <p className={mover.converged ? 'text-emerald-300' : 'text-muted'}>{mover.converged ? `Converged: pass ${mover.pass} copied 0` : `Pass ${mover.pass}${mover.last_key ? ` · cursor ${mover.last_key}` : ''}`}</p>
        {(mover.ranges ?? [{ name: 'all keys', cursor: mover.last_key, done: mover.copied + mover.skipped + mover.vanished + mover.failed, complete: mover.done }]).map((range) => <div key={range.name}><div className="flex justify-between text-xs text-muted"><span>{range.name}</span><span>{range.complete ? 'complete' : range.cursor || 'starting'}</span></div><div className="mt-2 h-2 overflow-hidden rounded-full bg-ink-700"><div className="h-full bg-ember-500" style={{ width: range.complete ? '100%' : '35%' }} /></div></div>)}
        <div><p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted">Fallback trend · completed windows</p><Sparkline label="Fallback reads" values={fallbackTrend.length ? fallbackTrend.map((point) => point.value) : [fallbackReads]} /></div>
      </div> : <p className="mt-4 text-sm text-muted">No mover has reported for this placement yet.</p>}
      {ledger.length > 0 && <div className="mt-5"><p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted">Ledger tail</p><div className="max-h-48 overflow-auto rounded-lg bg-ink-950 p-3 font-mono text-xs">{ledger.map((entry) => <p key={`${entry.at}-${entry.key}`} className="py-1"><span className={entry.result === 'error' ? 'text-red-300' : 'text-ember-300'}>{entry.result}</span> {entry.key} · {humanBytes(entry.size)} · {entry.mode}</p>)}</div></div>}
    </Card>

    <Card eyebrow="Step 7" title="Cutover">
      <div className="grid gap-4 sm:grid-cols-2"><label className="text-sm font-medium">Quiet window<select value={cutoverWindow} onChange={(event) => setCutoverWindow(event.target.value)} className={inputClass}><option value="5s">5 seconds (demo)</option><option value="30s">30 seconds</option><option value="60s">60 seconds</option><option value="5m">5 minutes</option></select></label><div className="rounded-lg bg-ink-950 p-3 text-sm"><p className="text-muted">In-flight multipart uploads on source</p><p className="mt-1 text-xl font-semibold">{detail.source_uploads_in_flight ?? 'unknown'}</p>{detail.source_uploads_error && <p className="mt-1 text-xs text-red-200">{detail.source_uploads_error}</p>}</div></div>
      <button type="button" disabled={busy || detail.state !== 'MIGRATING' || unfinished(operation)} onClick={() => void run('cutover', { window: cutoverWindow, wait: '30s' })} className="mt-4 rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50">Start cutover window</button>
      {operation?.kind === 'cutover' && progress && <p className="mt-3 text-sm text-muted">Window {progress.done} / {progress.total} {progress.unit}</p>}
    </Card>

    <Card eyebrow="Step 8" title="Purge or forget the source">
      <p className="text-sm text-muted">Purge deletes the source only after a server-side diff and confirmation token. Forget returns to ACTIVE but leaves the source bucket untouched.</p>
      <div className="mt-4 flex flex-wrap gap-3"><button type="button" disabled={busy || unfinished(operation)} onClick={() => void dryRunPurge()} className="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50">Dry-run purge</button><button type="button" disabled={busy || detail.state !== 'CUTOVER' || unfinished(operation)} onClick={() => void run('finish')} className="rounded-lg border border-ink-600 px-4 py-2 text-sm font-semibold disabled:opacity-50">Forget source without deleting</button>{detail.state === 'ACTIVE' && sourceIsTenantDefault && <button type="button" disabled={busy} onClick={() => { setBusy(true); void setTenantDefault(token, tenant, detail.primary).then(async () => { notify(`${detail.primary} is now ${tenant}'s default`); await refresh() }).catch((error) => notify(error instanceof Error ? error.message : String(error), 'danger')).finally(() => setBusy(false)) }} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold">Make {detail.primary} tenant default</button>}{canRemoveFormerSource && <button type="button" onClick={() => void dryRunRemove()} className="rounded-lg border border-red-600 px-4 py-2 text-sm font-semibold text-red-200">Remove {formerSource}</button>}</div>
    </Card>

    <ConfirmDrawer open={purge !== null} title={`Purge ${purge?.source ?? 'source bucket'}`} destructive busy={busy || unfinished(operation)} disabled={!purge?.allowed || !purge.token} onClose={() => setPurge(null)} onConfirm={() => { if (purge?.allowed && purge.token) { setFormerSource(purge.source ?? formerSource); void run('purge-source', { token: purge.token, wait: '30s' }).then(() => setPurge(null)) } }} summary={purge?.allowed ? <p>Delete <strong>{purge.objects.toLocaleString()} objects</strong> ({humanBytes(purge.bytes)}) and abort {purge.uploads_in_flight} uploads from <span className="font-mono text-paper">{purge.source}/{purge.bucket}</span>.</p> : <p className="text-red-200">{purge?.reason}</p>}><p className="text-xs text-muted">Confirmation token expires at {purge?.expires_at ? new Date(purge.expires_at).toLocaleTimeString() : '—'}.</p></ConfirmDrawer>

    <ConfirmDrawer open={remove !== null} title={`Remove ${formerSource}`} destructive busy={busy} disabled={!remove?.allowed || !remove.token} onClose={() => setRemove(null)} onConfirm={() => { if (remove?.allowed && remove.token) void removeCluster(token, formerSource, remove.token).then(async () => { notify(`Cluster ${formerSource} removed`); setRemove(null); await refresh() }).catch((error) => notify(error instanceof Error ? error.message : String(error), 'danger')) }} summary={remove?.allowed ? <p>No placements or tenant defaults depend on this cluster. Remove its directory record and stored secret.</p> : <p className="text-red-200">{remove?.reason}</p>} />
    {others.length > 0 && <Card eyebrow="Start another" title="Other buckets"><StartMigration placements={others} onPrepare={onPrepare} /></Card>}
  </div>
}
