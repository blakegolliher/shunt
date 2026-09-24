import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { adoptBucket, clearTarget, createBackendBucket, expandBucket, startOperation, getPlacementView, importClientKey, setPlacementReadOnly } from '../api/client'
import type { PlacementStatus, PlacementView } from '../api/client'
import { Card } from '../components/Card'
import { CopyLine } from '../components/CopyLine'
import { Drawer } from '../components/Drawer'
import { FenceStatus } from '../components/FenceStatus'
import { StateBadge } from '../components/StateBadge'
import { useStore } from '../store'
import { leadingShare } from '../hashRange'
import { OwnershipBar } from '../components/OwnershipBar'

const inputClass = 'mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper'
type Mode = 'adopt' | 'create'

function splitKey(key: string): [string, string] {
  const at = key.indexOf('/')
  return [key.slice(0, at), key.slice(at + 1)]
}

function generatedName(placement: PlacementStatus, target: string, placements: PlacementStatus[]) {
  const base = placement.names[placement.primary]
  const used = new Set(placements.map((item) => item.names[target]).filter(Boolean))
  for (let n = 1; ; n++) {
    let name = `${base}-${String(n).padStart(3, '0')}`
    if (name.length > 63) name = `${base.slice(0, 59).replace(/[.-]+$/, '')}-${String(n).padStart(3, '0')}`
    if (!used.has(name)) return name
  }
}

export function Buckets({ onMigrate }: { onMigrate: (key: string) => void }) {
  const { token, directory, refresh, notify } = useStore()
  const placements = useMemo(() => directory?.placements ?? [], [directory])
  const clusters = useMemo(() => directory?.clusters ?? [], [directory])
  const [adding, setAdding] = useState(false)
  const [mode, setMode] = useState<Mode>('adopt')
  const [tenant, setTenant] = useState('default')
  const [bucket, setBucket] = useState('')
  const [backend, setBackend] = useState('')
  const [cluster, setCluster] = useState('')
  const [keyAccess, setKeyAccess] = useState('')
  const [keySecret, setKeySecret] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<PlacementView | null>(null)
  const [importAccess, setImportAccess] = useState('')
  const [importSecret, setImportSecret] = useState('')
  const [importOnlyBucket, setImportOnlyBucket] = useState(false)
  const [target, setTarget] = useState('')
  const [expanding, setExpanding] = useState<string | null>(null)
  const [expandName, setExpandName] = useState('')
  const [acceptExisting, setAcceptExisting] = useState(false)
  const [spreadOn, setSpreadOn] = useState<string[]>([])
  // Move keys of a spread bucket (ADR-0018 N3): from one leg, a leading share of its range, to a cluster.
  const [moving, setMoving] = useState<string | null>(null)
  const [moveLeg, setMoveLeg] = useState('')
  const [moveShare, setMoveShare] = useState(0.5)
  const [moveTo, setMoveTo] = useState('')
  const [moveName, setMoveName] = useState('')
  const [moveRatio, setMoveRatio] = useState(0.01)
  // Consolidate a spread bucket (ADR-0018 N3c): every other leg's keys into the leg kept, one move each.
  const [consolidating, setConsolidating] = useState<string | null>(null)
  const [keepLeg, setKeepLeg] = useState('')
  const [consolidateRatio, setConsolidateRatio] = useState(0.01)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (!selected) return
    const [t, b] = splitKey(selected)
    void getPlacementView(token, t, b).then(setDetail).catch((error) => notify(error instanceof Error ? error.message : String(error), 'danger'))
  }, [selected, directory?.version, token, notify])

  const chosen = useMemo(() => placements.find((item) => item.key === selected), [placements, selected])
  // A tenant with no keys has every placement at 0 (a bucket-limited key still counts for its bucket).
  const tenantPlacements = placements.filter((item) => splitKey(item.key)[0] === tenant.trim())
  const tenantHasNoKeys = tenantPlacements.length > 0 && tenantPlacements.every((item) => item.client_keys === 0)
  const showKeyHint = mode === 'create' || tenantHasNoKeys
  const selectedCluster = cluster || clusters[0]?.name || ''
  const expandable = (placement: PlacementStatus) => placement.state === 'ACTIVE' && !placement.target && !placement.legs?.length && clusters.length > 0
  const spreading = mode === 'create' && spreadOn.length >= 2
  const movingBucket = placements.find((item) => item.key === moving)
  const sourceLeg = movingBucket?.legs?.find((l) => l.id === moveLeg)
  const destLeg = movingBucket?.legs?.find((l) => l.cluster === moveTo && l.id !== moveLeg) // a leg to move into; none: a new one
  const openMove = (placement: PlacementStatus) => {
    const legs = placement.legs?.filter((l) => l.ranges.length > 0) ?? []
    const from = legs[0]
    setMoveLeg(from?.id ?? ''); setMoveShare(0.5); setMoveRatio(0.01); setMoveName('')
    setMoveTo(clusters.find((c) => c.name !== from?.cluster)?.name ?? from?.cluster ?? ''); setMoving(placement.key)
  }
  const startMove = async () => {
    if (!movingBucket || !sourceLeg) return
    const range = leadingShare(sourceLeg.ranges[0], moveShare)
    const key = movingBucket.key
    const [, b] = splitKey(key)
    const args = { to: moveTo, ...(destLeg ? {} : { name: moveName.trim() || b }), range, ratio: moveRatio, create: true, wait: '30s' }
    if (await act(() => startOperation(token, 'ramp', key, args), `${key}: moving ${Math.round(moveShare * 100)}% of leg ${sourceLeg.id} to ${moveTo}`)) { setMoving(null); onMigrate(key) }
  }
  const consolidatingBucket = placements.find((item) => item.key === consolidating)
  const kept = consolidatingBucket?.legs?.find((l) => l.id === keepLeg)
  const toMove = consolidatingBucket?.legs?.filter((l) => l.id !== keepLeg && l.ranges.length > 0) ?? []
  const movesLeft = toMove.reduce((n, l) => n + l.ranges.length, 0)
  const openConsolidate = (placement: PlacementStatus) => {
    const largest = [...(placement.legs ?? [])].sort((a, b) => b.share - a.share)[0]
    setKeepLeg(largest?.id ?? ''); setConsolidateRatio(0.01); setConsolidating(placement.key)
  }
  const consolidateNext = async () => {
    const next = toMove[0]
    if (!consolidatingBucket || !kept || !next) return
    const key = consolidatingBucket.key
    const args = { to: kept.cluster, name: kept.bucket, leg: next.id, ratio: consolidateRatio, wait: '30s' }
    if (await act(() => startOperation(token, 'ramp', key, args), `${key}: moving leg ${next.id}'s keys into ${kept.cluster}/${kept.bucket}`)) { setConsolidating(null); onMigrate(key) }
  }
  const idleLegs = (placement: PlacementStatus) => placement.state === 'ACTIVE' && !placement.move ? (placement.legs ?? []).filter((l) => l.ranges.length === 0) : []
  const growing = useMemo(() => placements.find((item) => item.key === expanding), [placements, expanding])
  // Another cluster, or another bucket on the bucket's own cluster (ADR-0018 N3b), listed last.
  const targets = growing ? [...clusters.filter((item) => item.name !== growing.primary), ...clusters.filter((item) => item.name === growing.primary)] : []
  const sameCluster = Boolean(growing && target === growing.primary)
  const generated = growing && target ? generatedName(growing, target, placements) : ''
  const openExpand = (placement: PlacementStatus, prefer = '', name = '') => {
    const to = prefer && prefer !== placement.primary && clusters.some((item) => item.name === prefer) ? prefer : clusters.find((item) => item.name !== placement.primary)?.name ?? ''
    setTarget(to); setExpandName(name); setAcceptExisting(false); setExpanding(placement.key)
  }
  // The client bucket the add form names, when the tenant already has it: that is expand, not add.
  const existing = placements.find((item) => item.key === `${tenant.trim()}/${bucket.trim()}`)
  const tenantBuckets = placements.map((item) => splitKey(item.key)).filter(([t]) => t === tenant.trim()).map(([, b]) => b)
  const act = async (fn: () => Promise<unknown>, success: string) => {
    setBusy(true)
    try { await fn(); notify(success); await refresh(); return true } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger'); return false } finally { setBusy(false) }
  }
  const resetImport = () => { setImportAccess(''); setImportSecret(''); setImportOnlyBucket(false) }
  const importKey = async (event: FormEvent) => {
    event.preventDefault()
    if (!detail) return
    const [t, b] = splitKey(detail.key)
    const accessKey = importAccess.trim()
    if (await act(() => importClientKey(token, t, { access_key: accessKey, secret: importSecret, cluster: detail.primary, buckets: importOnlyBucket ? [b] : undefined }), `Client key ${accessKey} imported for ${importOnlyBucket ? detail.key : `tenant ${t}`}`)) resetImport()
  }
  const resetAdd = () => { setAdding(false); setBucket(''); setBackend(''); setKeyAccess(''); setKeySecret(''); setSpreadOn([]) }
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (existing) return
    const name = backend.trim() || bucket.trim()
    const keys = keyAccess && keySecret ? [{ access_key: keyAccess.trim(), secret: keySecret }] : undefined
    const ok = mode === 'adopt'
      ? await act(() => adoptBucket(token, tenant.trim(), bucket.trim(), { cluster: selectedCluster, name, keys }), `Bucket ${tenant}/${bucket} adopted`)
      : await act(() => createBackendBucket(token, tenant.trim(), bucket.trim(), spreading ? { cluster: spreadOn[0], name, keys, legs: spreadOn.map((c) => ({ cluster: c, name })) } : { cluster: selectedCluster, name, keys }), spreading ? `Bucket ${tenant}/${bucket} created, spread over ${spreadOn.length} clusters` : `Bucket ${tenant}/${bucket} created`)
    if (ok) resetAdd()
  }

  return <div className="space-y-5">
    <Card eyebrow="Directory" title="Buckets" action={<button type="button" onClick={() => setAdding(true)} className="rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white hover:bg-ember-500">Adopt or create</button>}>
      {placements.length === 0 ? <p className="text-sm text-muted">No bucket placements are recorded.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Tenant</th><th className="pb-3">Bucket</th><th className="pb-3">State</th><th className="pb-3">Primary cluster</th><th className="pb-3">Backend name</th><th className="pb-3">Writes</th><th className="pb-3"><span className="sr-only">Actions</span></th></tr></thead>
        <tbody className="divide-y divide-ink-700">{placements.map((placement) => { const [t, b] = splitKey(placement.key); return <tr key={placement.key} onClick={() => setSelected(placement.key)} className="cursor-pointer hover:bg-ink-800/60"><td className="py-3 text-muted">{t}</td><td className="py-3 font-semibold text-ember-300">{b}{placement.client_keys === 0 && <span className="ml-2 rounded-full border border-amber-500/60 bg-amber-950/40 px-2 py-0.5 text-xs font-normal text-amber-200">no client keys</span>}</td><td className="py-3"><StateBadge state={placement.state} /></td><td className="py-3">{placement.legs?.length ? <div className="min-w-40"><span title="Keys split by hash across these clusters">spread: {placement.legs.map((l) => l.cluster).join(' + ')}</span><div className="mt-1.5"><OwnershipBar legs={placement.legs} move={placement.move} compact /></div></div> : placement.primary}</td><td className="py-3 font-mono text-muted">{placement.legs?.length ? placement.legs.map((l) => l.bucket).join(', ') : placement.names[placement.primary]}</td><td className={`py-3 ${placement.read_only ? 'text-amber-200' : 'text-emerald-300'}`}>{placement.read_only ? 'read-only' : 'enabled'}</td><td className="py-3 text-right">{placement.state === 'ACTIVE' && (placement.legs?.length ?? 0) > 1 && <button type="button" aria-label={`Move keys of ${placement.key}`} onClick={(event) => { event.stopPropagation(); openMove(placement) }} className="mr-2 rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold hover:bg-ember-600 hover:text-white">Move keys</button>}{placement.state === 'ACTIVE' && (placement.legs?.length ?? 0) > 1 && <button type="button" aria-label={`Consolidate ${placement.key}`} onClick={(event) => { event.stopPropagation(); openConsolidate(placement) }} className="mr-2 rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold hover:bg-ember-600 hover:text-white">Consolidate</button>}{idleLegs(placement).length > 0 && <button type="button" aria-label={`Retire idle legs of ${placement.key}`} onClick={(event) => { event.stopPropagation(); const [t, b] = splitKey(placement.key); const idle = idleLegs(placement).map((l) => `${l.cluster}/${l.bucket}`).join(', '); void act(() => clearTarget(token, t, b), `${placement.key}: retired ${idle}, which owned no keys; the buckets are still there`) }} disabled={busy} className="mr-2 rounded-lg border border-ink-600 px-3 py-1.5 text-xs font-semibold text-muted hover:text-paper">Retire idle leg</button>}{placement.state === 'ACTIVE' && placement.target && <button type="button" aria-label={`Clear target of ${placement.key}`} onClick={(event) => { event.stopPropagation(); const [t, b] = splitKey(placement.key); void act(() => clearTarget(token, t, b), `${placement.key}: target ${placement.target} cleared; its bucket is still there`) }} disabled={busy} className="rounded-lg border border-ink-600 px-3 py-1.5 text-xs font-semibold text-muted hover:text-paper">Clear target</button>}{expandable(placement) && <button type="button" aria-label={`Expand ${placement.key}`} onClick={(event) => { event.stopPropagation(); openExpand(placement) }} className="rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold hover:bg-ember-600 hover:text-white">Expand</button>}</td></tr> })}</tbody>
      </table></div>}
    </Card>

    <Drawer open={adding} title="Add a bucket" eyebrow="Buckets" onClose={resetAdd} footer={<button form="bucket-add" type="submit" disabled={busy || !tenant.trim() || !bucket.trim() || !selectedCluster || Boolean(existing)} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">{mode === 'adopt' ? 'Adopt bucket' : 'Create bucket'}</button>}>
      <form id="bucket-add" onSubmit={(event) => void submit(event)} className="grid gap-4">
        <div className="grid grid-cols-2 gap-2 rounded-lg bg-ink-950 p-1"><button type="button" onClick={() => setMode('adopt')} className={`rounded-md px-3 py-2 text-sm ${mode === 'adopt' ? 'bg-ember-600 text-white' : 'text-muted'}`}>Adopt existing</button><button type="button" onClick={() => setMode('create')} className={`rounded-md px-3 py-2 text-sm ${mode === 'create' ? 'bg-ember-600 text-white' : 'text-muted'}`}>Create new</button></div>
        <div className="grid grid-cols-2 gap-3"><label className="text-sm font-medium">Tenant<input value={tenant} onChange={(event) => setTenant(event.target.value)} className={inputClass} /></label><label className="text-sm font-medium">Client bucket<input value={bucket} onChange={(event) => setBucket(event.target.value)} className={inputClass} list="client-buckets" placeholder="new name, or pick one" autoComplete="off" /></label><datalist id="client-buckets">{tenantBuckets.map((name) => <option key={name} value={name} />)}</datalist></div>
        {mode === 'create' && clusters.length > 1 && <fieldset className="rounded-lg border border-ink-700 p-4"><legend className="px-2 text-sm font-medium">Spread across clusters <span className="text-muted">(optional)</span></legend><p className="mb-2 text-xs text-muted">Pick two or more: one new bucket on each, and every key lives on exactly one of them, chosen by its hash. Listings merge them all. Move keys moves part of one to another later, and Consolidate brings them back to one bucket.</p><div className="flex flex-wrap gap-3">{clusters.map((item) => <label key={item.name} className="flex items-center gap-2 text-sm"><input type="checkbox" checked={spreadOn.includes(item.name)} onChange={(event) => setSpreadOn((old) => event.target.checked ? [...old, item.name] : old.filter((c) => c !== item.name))} />{item.name}</label>)}</div></fieldset>}
        <label className="text-sm font-medium">Cluster{spreading && <span className="text-muted"> (not used: spreading)</span>}<select value={selectedCluster} disabled={spreading} onChange={(event) => setCluster(event.target.value)} className={inputClass}>{clusters.map((item) => <option key={item.name}>{item.name}</option>)}</select></label>
        <label className="text-sm font-medium">Backend bucket name <span className="text-muted">(defaults to client name)</span><input value={backend} onChange={(event) => setBackend(event.target.value)} className={inputClass} placeholder={bucket || 'bucket-name'} /></label>
        {existing && <div role="status" className="rounded-lg border border-ember-500 bg-ink-950 p-4 text-sm">
          <p><span className="font-mono text-paper">{existing.key}</span> already exists: {existing.legs?.length ? <>{existing.state}, spread over {existing.legs.map((l) => l.cluster).join(' + ')}</> : <>{existing.state} on {existing.primary} as <span className="font-mono">{existing.names[existing.primary]}</span></>}. Clients keep one name per bucket, so adding a cluster to it is Expand, not {mode === 'adopt' ? 'Adopt' : 'Create'}.</p>
          {existing.legs?.length
            ? <p className="mt-2 text-muted">It is spread over {existing.legs.length} backend buckets: Move keys adds one by moving part of a leg to it, and Consolidate brings it back to one.</p>
            : expandable(existing)
            ? <button type="button" onClick={() => { const found = existing; const to = selectedCluster !== existing.primary ? selectedCluster : clusters.find((item) => item.name !== existing.primary)?.name; const name = backend.trim(); resetAdd(); openExpand(found, to, name) }} className="mt-3 rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white">Expand {splitKey(existing.key)[1]} to {selectedCluster !== existing.primary ? selectedCluster : clusters.find((item) => item.name !== existing.primary)?.name}</button>
            : existing.target || existing.state !== 'ACTIVE'
              ? <button type="button" onClick={() => { const key = existing.key; resetAdd(); onMigrate(key) }} className="mt-3 rounded-lg border border-ember-500 px-4 py-2 font-semibold">Continue its migration</button>
              : <p className="mt-2 text-muted">Add another cluster first; expand needs one other than {existing.primary}.</p>}
        </div>}
        <fieldset className="rounded-lg border border-ink-700 p-4"><legend className="px-2 text-sm font-medium">Optional client key import</legend>{showKeyHint && <p className="mb-3 text-xs text-amber-200">Clients reach a bucket through shunt only with a client key shunt holds{tenantHasNoKeys ? `, and tenant ${tenant.trim()} has none yet` : ''}. Import one the cluster knows, or add one later with shunt client add.</p>}<p className="mb-3 text-xs text-muted">The secret is checked against the cluster and stored by reference in the control plane; it is never returned.</p><label className="text-sm">Access key<input value={keyAccess} onChange={(event) => setKeyAccess(event.target.value)} className={inputClass} autoComplete="off" /></label><label className="mt-3 block text-sm">Secret<input type="password" value={keySecret} onChange={(event) => setKeySecret(event.target.value)} className={inputClass} autoComplete="off" /></label></fieldset>
      </form>
    </Drawer>

    <Drawer open={Boolean(growing)} title={`Expand ${expanding ?? ''}`} eyebrow="Prepare migration" onClose={() => { setExpanding(null); setExpandName(''); setAcceptExisting(false) }}>
      <div className="grid gap-4"><p className="text-sm text-muted">Creates the bucket on the target cluster, measures its conditional-write support, and records it as this bucket's migration target. Client traffic is unchanged until you ramp.</p><label className="text-sm font-medium">Target cluster<select value={target} onChange={(event) => setTarget(event.target.value)} className={inputClass}>{targets.map((item) => <option key={item.name} value={item.name}>{item.name}{growing && item.name === growing.primary ? ' (another bucket here)' : ''}</option>)}</select></label>{sameCluster && <p className="text-xs text-amber-200">A bucket on its own cluster cannot be recorded as a target: this starts the move at once, 1% of writes to the new bucket, and Migrations takes it from there.</p>}<label className="text-sm font-medium">Target bucket name<input value={expandName} onChange={(event) => setExpandName(event.target.value)} className={`${inputClass} font-mono`} placeholder={generated || 'Choose a target'} autoComplete="off" /></label><p className="text-xs text-muted">Leave empty for <span className="font-mono text-paper">{generated || '—'}</span>. A bucket of this name that already exists on {target || 'the target'} is used as it is; otherwise it is created, which the cluster's key must be allowed to do.</p>{!sameCluster && <label className="flex items-start gap-2 text-xs text-muted"><input type="checkbox" className="mt-0.5" checked={acceptExisting} onChange={(event) => setAcceptExisting(event.target.checked)} /><span>The target bucket already holds this bucket's objects, copied ahead. Without this, a target bucket with any object is refused: its objects would mix into this bucket.</span></label>}<button type="button" disabled={busy || !target} onClick={() => { if (!growing) return; const key = growing.key; const [t, b] = splitKey(key); const name = expandName.trim() || generated; const start = sameCluster ? () => startOperation(token, 'ramp', key, { to: target, name, ratio: 0.01, create: true, wait: '30s' }) : () => expandBucket(token, t, b, target, name, acceptExisting); void act(start, sameCluster ? `${key}: moving to ${target}/${name}` : `${key} expanded to ${target}`).then((ok) => { if (ok) { setExpanding(null); setExpandName(''); setAcceptExisting(false); onMigrate(key) } }) }} className="mt-4 w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">{sameCluster ? 'Start the move and continue to Migrations' : 'Create target and continue to Migrations'}</button></div>
    </Drawer>

    <Drawer open={Boolean(movingBucket)} title={`Move keys of ${moving ?? ''}`} eyebrow="Move part of a bucket" onClose={() => setMoving(null)}>
      {movingBucket && <div className="grid gap-4">
        <p className="text-sm text-muted">Moves part of one leg's keys to another cluster, through the steps of a migration: ramp, migrate, mover, cutover, purge. The rest of the bucket stays where it is.</p>
        <label className="text-sm font-medium">From leg<select value={moveLeg} onChange={(event) => setMoveLeg(event.target.value)} className={inputClass}>{movingBucket.legs?.filter((l) => l.ranges.length > 0).map((l) => <option key={l.id} value={l.id}>{l.cluster} ({Math.round(l.share * 1000) / 10}% of keys)</option>)}</select></label>
        <label className="text-sm font-medium">Share of its keys to move: {Math.round(moveShare * 100)}%<input aria-label="Share of the leg's keys" type="range" min="0.01" max="1" step="0.01" value={moveShare} onChange={(event) => setMoveShare(Number(event.target.value))} className="mt-2 w-full accent-ember-500" /></label>
        {sourceLeg && sourceLeg.ranges.length > 1 && <p className="text-xs text-muted">This leg owns {sourceLeg.ranges.length} ranges; the share is of its first.</p>}
        <label className="text-sm font-medium">To cluster<select value={moveTo} onChange={(event) => setMoveTo(event.target.value)} className={inputClass}>{clusters.map((c) => <option key={c.name} value={c.name}>{c.name}{c.name === sourceLeg?.cluster ? ' (a new bucket beside it)' : movingBucket.legs?.some((l) => l.cluster === c.name) ? ' (a leg already)' : ''}</option>)}</select></label>
        {destLeg ? <p className="text-sm text-muted">Into its leg's bucket <span className="font-mono text-paper">{destLeg.bucket}</span>.</p> : <label className="text-sm font-medium">New leg's bucket name<input value={moveName} onChange={(event) => setMoveName(event.target.value)} className={`${inputClass} font-mono`} placeholder={splitKey(movingBucket.key)[1]} autoComplete="off" /></label>}
        <div><p className="text-sm font-medium">First step: writes of the moving keys to the new leg</p><div className="mt-2 flex flex-wrap gap-2">{[0.01, 0.25, 0.5, 1].map((r) => <button key={r} type="button" onClick={() => setMoveRatio(r)} className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${moveRatio === r ? 'border-ember-400 bg-ember-600 text-white' : 'border-ink-700 text-muted'}`}>{r * 100}%</button>)}</div></div>
        <button type="button" disabled={busy || !sourceLeg || !moveTo || (!destLeg && moveTo === sourceLeg.cluster && !moveName.trim())} onClick={() => void startMove()} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Start the move and continue to Migrations</button>
      </div>}
    </Drawer>

    <Drawer open={Boolean(consolidatingBucket)} title={`Consolidate ${consolidating ?? ''}`} eyebrow="Back to one bucket" onClose={() => setConsolidating(null)}>
      {consolidatingBucket && <div className="grid gap-4">
        <p className="text-sm text-muted">Moves every other leg's keys into the one you keep, one move at a time, each through ramp, migrate, mover, cutover and purge. When one leg owns every key the bucket is a plain bucket again, and step-out can take shunt out of its path.</p>
        <label className="text-sm font-medium">Keep<select value={keepLeg} onChange={(event) => setKeepLeg(event.target.value)} className={inputClass}>{consolidatingBucket.legs?.map((l) => <option key={l.id} value={l.id}>{l.cluster}/{l.bucket} ({Math.round(l.share * 1000) / 10}% of keys)</option>)}</select></label>
        <div className="rounded-lg border border-ink-700 p-4 text-sm"><p className="font-medium">{movesLeft} {movesLeft === 1 ? 'move' : 'moves'} left</p><ul className="mt-2 grid gap-1 text-muted">{toMove.map((l) => <li key={l.id}><span className="font-mono text-paper">{l.cluster}/{l.bucket}</span>: {Math.round(l.share * 1000) / 10}% of keys{l.ranges.length > 1 ? `, in ${l.ranges.length} ranges (a move each)` : ''}</li>)}</ul>{toMove[0] && <p className="mt-2 text-xs text-muted">Next: <span className="font-mono">{toMove[0].cluster}/{toMove[0].bucket}</span>. Come back here when its move has been purged to start the next.</p>}</div>
        <div><p className="text-sm font-medium">First step: writes of the moving keys to the kept leg</p><div className="mt-2 flex flex-wrap gap-2">{[0.01, 0.25, 0.5, 1].map((r) => <button key={r} type="button" onClick={() => setConsolidateRatio(r)} className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${consolidateRatio === r ? 'border-ember-400 bg-ember-600 text-white' : 'border-ink-700 text-muted'}`}>{r * 100}%</button>)}</div></div>
        <button type="button" disabled={busy || !kept || toMove.length === 0} onClick={() => void consolidateNext()} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Start the next move and continue to Migrations</button>
      </div>}
    </Drawer>

    <Drawer open={Boolean(selected)} title={selected ?? ''} eyebrow="Bucket detail" onClose={() => { setSelected(null); setDetail(null); resetImport() }}>
      {detail ? <div className="space-y-6">
        <div className="flex items-center justify-between"><StateBadge state={detail.state} /><span className={detail.read_only ? 'text-amber-200' : 'text-emerald-300'}>{detail.read_only ? 'read-only' : 'writes enabled'}</span></div>
        {chosen?.client_keys === 0 && <div role="alert" className="rounded-lg border border-amber-500/60 bg-amber-950/40 p-4 text-sm text-amber-100">
          <p>No client key can reach this bucket, so shunt refuses every request for it. Import a key that {detail.primary} knows; its secret is checked against {detail.primary} before it is stored.</p>
          <form onSubmit={(event) => void importKey(event)} className="mt-3 grid gap-3">
            <label className="text-sm">Client access key<input value={importAccess} onChange={(event) => setImportAccess(event.target.value)} className={inputClass} autoComplete="off" /></label>
            <label className="text-sm">Client secret<input type="password" value={importSecret} onChange={(event) => setImportSecret(event.target.value)} className={inputClass} autoComplete="off" /></label>
            <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={importOnlyBucket} onChange={(event) => setImportOnlyBucket(event.target.checked)} /> Limit this key to {splitKey(detail.key)[1]} (otherwise it reaches every bucket of tenant {splitKey(detail.key)[0]})</label>
            <button type="submit" disabled={busy || !importAccess.trim() || !importSecret} className="rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Import client key</button>
          </form>
          <p className="mt-4 text-xs text-amber-200/80">Or from a shell (it prompts for the secret; pass the control token with --token-ref or SHUNT_API_TOKEN_REF):</p>
          <div className="mt-2"><CopyLine value={`shunt client add <access-key> --tenant ${splitKey(detail.key)[0]} --check ${detail.primary} --api ${window.location.origin}`} /></div>
        </div>}
        <FenceStatus held={detail.fence.held} version={detail.fence.version} waitingOn={detail.fence.waiting_on} />
        {detail.legs?.length ? <Card title="Legs"><OwnershipBar legs={detail.legs} move={detail.move} /><dl className="mt-4 grid gap-3 text-sm">{detail.legs.map((l) => <div key={l.id} className="flex justify-between gap-4"><dt>{l.cluster}{l.id !== l.cluster && <span className="text-muted"> (leg {l.id})</span>}</dt><dd className="text-right font-mono text-muted">{l.bucket} · {l.ranges.length ? `${Math.round(l.share * 1000) / 10}% of keys` : 'idle'}{l.ranges.map((r) => <span key={r.from} className="block text-xs">{r.from}–{r.to}</span>)}</dd></div>)}</dl></Card> : null}
        <Card title="Placement names"><dl className="grid gap-3 text-sm">{Object.entries(detail.names).map(([name, value]) => <div key={name} className="flex justify-between gap-4"><dt>{name}</dt><dd className="font-mono text-muted">{value}</dd></div>)}</dl></Card>
        <div className="flex flex-wrap gap-3"><button type="button" disabled={busy} onClick={() => { const [t, b] = splitKey(detail.key); void act(() => setPlacementReadOnly(token, t, b, !detail.read_only), `${detail.key} is ${detail.read_only ? 'writable' : 'read-only'}`) }} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold">Make {detail.read_only ? 'writable' : 'read-only'}</button></div>
      </div> : <p className="text-muted">Loading bucket detail…</p>}
    </Drawer>
  </div>
}
