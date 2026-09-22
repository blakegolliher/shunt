import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { adoptBucket, createBackendBucket, expandBucket, getPlacementView, setPlacementReadOnly } from '../api/client'
import type { PlacementStatus, PlacementView } from '../api/client'
import { Card } from '../components/Card'
import { Drawer } from '../components/Drawer'
import { FenceStatus } from '../components/FenceStatus'
import { StateBadge } from '../components/StateBadge'
import { useStore } from '../store'

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
  const [target, setTarget] = useState('')
  const [expanding, setExpanding] = useState(false)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (!selected) return
    const [t, b] = splitKey(selected)
    void getPlacementView(token, t, b).then(setDetail).catch((error) => notify(error instanceof Error ? error.message : String(error), 'danger'))
  }, [selected, directory?.version, token, notify])

  const chosen = useMemo(() => placements.find((item) => item.key === selected), [placements, selected])
  const selectedCluster = cluster || clusters[0]?.name || ''
  const targets = chosen ? clusters.filter((item) => item.name !== chosen.primary) : []
  const generated = chosen && target ? generatedName(chosen, target, placements) : ''
  const act = async (fn: () => Promise<unknown>, success: string) => {
    setBusy(true)
    try { await fn(); notify(success); await refresh(); return true } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger'); return false } finally { setBusy(false) }
  }
  const resetAdd = () => { setAdding(false); setBucket(''); setBackend(''); setKeyAccess(''); setKeySecret('') }
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    const name = backend.trim() || bucket.trim()
    const ok = mode === 'adopt'
      ? await act(() => adoptBucket(token, tenant.trim(), bucket.trim(), { cluster: selectedCluster, name, keys: keyAccess && keySecret ? [{ access_key: keyAccess, secret: keySecret }] : undefined }), `Bucket ${tenant}/${bucket} adopted`)
      : await act(() => createBackendBucket(token, tenant.trim(), bucket.trim(), { cluster: selectedCluster, name }), `Bucket ${tenant}/${bucket} created`)
    if (ok) resetAdd()
  }

  return <div className="space-y-5">
    <Card eyebrow="Directory" title="Buckets" action={<button type="button" onClick={() => setAdding(true)} className="rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white hover:bg-ember-500">Adopt or create</button>}>
      {placements.length === 0 ? <p className="text-sm text-muted">No bucket placements are recorded.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Tenant</th><th className="pb-3">Bucket</th><th className="pb-3">State</th><th className="pb-3">Primary cluster</th><th className="pb-3">Backend name</th><th className="pb-3">Writes</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{placements.map((placement) => { const [t, b] = splitKey(placement.key); return <tr key={placement.key} onClick={() => setSelected(placement.key)} className="cursor-pointer hover:bg-ink-800/60"><td className="py-3 text-muted">{t}</td><td className="py-3 font-semibold text-ember-300">{b}</td><td className="py-3"><StateBadge state={placement.state} /></td><td className="py-3">{placement.primary}</td><td className="py-3 font-mono text-muted">{placement.names[placement.primary]}</td><td className={`py-3 ${placement.read_only ? 'text-amber-200' : 'text-emerald-300'}`}>{placement.read_only ? 'read-only' : 'enabled'}</td></tr> })}</tbody>
      </table></div>}
    </Card>

    <Drawer open={adding} title="Add a bucket" eyebrow="Buckets" onClose={resetAdd} footer={<button form="bucket-add" type="submit" disabled={busy || !tenant.trim() || !bucket.trim() || !selectedCluster} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">{mode === 'adopt' ? 'Adopt bucket' : 'Create bucket'}</button>}>
      <form id="bucket-add" onSubmit={(event) => void submit(event)} className="grid gap-4">
        <div className="grid grid-cols-2 gap-2 rounded-lg bg-ink-950 p-1"><button type="button" onClick={() => setMode('adopt')} className={`rounded-md px-3 py-2 text-sm ${mode === 'adopt' ? 'bg-ember-600 text-white' : 'text-muted'}`}>Adopt existing</button><button type="button" onClick={() => setMode('create')} className={`rounded-md px-3 py-2 text-sm ${mode === 'create' ? 'bg-ember-600 text-white' : 'text-muted'}`}>Create new</button></div>
        <div className="grid grid-cols-2 gap-3"><label className="text-sm font-medium">Tenant<input value={tenant} onChange={(event) => setTenant(event.target.value)} className={inputClass} /></label><label className="text-sm font-medium">Client bucket<input value={bucket} onChange={(event) => setBucket(event.target.value)} className={inputClass} /></label></div>
        <label className="text-sm font-medium">Cluster<select value={selectedCluster} onChange={(event) => setCluster(event.target.value)} className={inputClass}>{clusters.map((item) => <option key={item.name}>{item.name}</option>)}</select></label>
        <label className="text-sm font-medium">Backend bucket name <span className="text-muted">(defaults to client name)</span><input value={backend} onChange={(event) => setBackend(event.target.value)} className={inputClass} placeholder={bucket || 'bucket-name'} /></label>
        {mode === 'adopt' && <fieldset className="rounded-lg border border-ink-700 p-4"><legend className="px-2 text-sm font-medium">Optional client key import</legend><p className="mb-3 text-xs text-muted">The secret is checked against the cluster and stored by reference in the control plane; it is never returned.</p><label className="text-sm">Access key<input value={keyAccess} onChange={(event) => setKeyAccess(event.target.value)} className={inputClass} autoComplete="off" /></label><label className="mt-3 block text-sm">Secret<input type="password" value={keySecret} onChange={(event) => setKeySecret(event.target.value)} className={inputClass} autoComplete="off" /></label></fieldset>}
      </form>
    </Drawer>

    <Drawer open={Boolean(selected)} title={selected ?? ''} eyebrow="Bucket detail" onClose={() => { setSelected(null); setDetail(null); setExpanding(false) }}>
      {detail ? <div className="space-y-6">
        <div className="flex items-center justify-between"><StateBadge state={detail.state} /><span className={detail.read_only ? 'text-amber-200' : 'text-emerald-300'}>{detail.read_only ? 'read-only' : 'writes enabled'}</span></div>
        <FenceStatus held={detail.fence.held} version={detail.fence.version} waitingOn={detail.fence.waiting_on} />
        <Card title="Placement names"><dl className="grid gap-3 text-sm">{Object.entries(detail.names).map(([name, value]) => <div key={name} className="flex justify-between gap-4"><dt>{name}</dt><dd className="font-mono text-muted">{value}</dd></div>)}</dl></Card>
        <div className="flex flex-wrap gap-3"><button type="button" disabled={busy} onClick={() => { const [t, b] = splitKey(detail.key); void act(() => setPlacementReadOnly(token, t, b, !detail.read_only), `${detail.key} is ${detail.read_only ? 'writable' : 'read-only'}`) }} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold">Make {detail.read_only ? 'writable' : 'read-only'}</button>{detail.state === 'ACTIVE' && !detail.target && <button type="button" onClick={() => { setTarget(targets[0]?.name ?? ''); setExpanding(true) }} className="rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white">Expand</button>}</div>
        {expanding && <Card eyebrow="Prepare migration" title="Expand to another cluster"><label className="text-sm font-medium">Target cluster<select value={target} onChange={(event) => setTarget(event.target.value)} className={inputClass}>{targets.map((item) => <option key={item.name}>{item.name}</option>)}</select></label><div className="mt-4 rounded-lg bg-ink-950 p-3 text-sm"><p className="text-muted">Generated backend name</p><p className="mt-1 font-mono text-paper">{generated || 'Choose a target'}</p></div><button type="button" disabled={busy || !target} onClick={() => { const [t, b] = splitKey(detail.key); void act(() => expandBucket(token, t, b, target), `${detail.key} expanded to ${target}`).then((ok) => { if (ok) { setSelected(null); onMigrate(detail.key) } }) }} className="mt-4 w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Create target and continue to Migrations</button></Card>}
      </div> : <p className="text-muted">Loading bucket detail…</p>}
    </Drawer>
  </div>
}
