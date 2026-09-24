import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { addCluster, getClusterView, getTelemetrySeries, probeCluster, removeCluster, removeClusterDryRun, setClusterReadOnly, setTenantDefault } from '../api/client'
import type { ClusterInput, ClusterProbeResult, ClusterStatus, ClusterView, RemoveDryRun } from '../api/client'
import { Card } from '../components/Card'
import { Drawer } from '../components/Drawer'
import { Sparkline } from '../components/Sparkline'
import { useStore } from '../store'

const inputClass = 'mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper'

interface FormState { name: string; endpoint: string; scheme: string; region: string; accessKey: string; secretMode: 'value' | 'ref'; secret: string }
const emptyForm: FormState = { name: '', endpoint: '', scheme: 'http', region: 'us-east-1', accessKey: '', secretMode: 'value', secret: '' }

function clusterInput(form: FormState): ClusterInput {
  const input: ClusterInput = { name: form.name.trim(), cluster: { scheme: form.scheme, region: form.region.trim(), endpoints: [form.endpoint.trim()], credentials: { access_key: form.accessKey.trim() } } }
  if (form.secretMode === 'value') input.secret = form.secret
  else input.cluster.credentials.secret_ref = form.secret.trim()
  return input
}

function profile(cluster: ClusterStatus, view?: ClusterView) {
  if (!view) return cluster.conditional_write || cluster.conditional_delete ? 'assumed' : 'unknown'
  return view.capabilities.conditional_write.known && view.capabilities.conditional_delete.known ? 'measured' : 'assumed'
}

export function Clusters() {
  const { token, directory, refresh, notify } = useStore()
  const clusters = directory?.clusters ?? []
  const [views, setViews] = useState<Record<string, ClusterView>>({})
  const [adding, setAdding] = useState(false)
  const [form, setForm] = useState<FormState>(emptyForm)
  const [probe, setProbe] = useState<ClusterProbeResult | null>(null)
  const [probedInput, setProbedInput] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const [history, setHistory] = useState<number[]>([])
  const [busy, setBusy] = useState(false)
  const [removal, setRemoval] = useState<RemoveDryRun | null>(null)
  const [newDefault, setNewDefault] = useState<Record<string, string>>({}) // tenant → the cluster chosen to replace this one as its default

  useEffect(() => {
    let live = true
    void Promise.all(clusters.map(async (cluster) => [cluster.name, await getClusterView(token, cluster.name)] as const)).then((entries) => { if (live) setViews(Object.fromEntries(entries)) }).catch(() => undefined)
    return () => { live = false }
  }, [clusters.map((cluster) => cluster.name).join('|'), directory?.version, token]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!selected) return
    const query = new URLSearchParams({ scope: `cluster:${selected}`, series: 'client_total', op: 'other' })
    void getTelemetrySeries(token, query).then((series) => setHistory(series.points.map((point) => point.p99_us ?? 0))).catch(() => setHistory([]))
  }, [selected, token])

  const currentInput = useMemo(() => JSON.stringify(clusterInput(form)), [form])
  const update = <K extends keyof FormState>(key: K, value: FormState[K]) => { setForm((old) => ({ ...old, [key]: value })); setProbe(null) }
  const act = async (fn: () => Promise<unknown>, success: string) => {
    setBusy(true)
    try { await fn(); notify(success); await refresh(); return true } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger'); return false } finally { setBusy(false) }
  }

  const onProbe = async () => {
    setBusy(true)
    try { const result = await probeCluster(token, clusterInput(form)); setProbe(result); setProbedInput(currentInput); notify(`Probe reached ${result.cluster.name}`) } catch (error) { notify(error instanceof Error ? error.message : String(error), 'danger') } finally { setBusy(false) }
  }
  const onSave = async (event: FormEvent) => {
    event.preventDefault()
    if (!probe || probedInput !== currentInput) return
    if (await act(() => addCluster(token, clusterInput(form)), `Cluster ${form.name} added`)) { setAdding(false); setForm(emptyForm); setProbe(null) }
  }
  const detail = selected ? views[selected] : undefined

  return <div className="space-y-5">
    <Card eyebrow="Backends" title="Clusters" action={<button type="button" onClick={() => setAdding(true)} className="rounded-lg bg-ember-600 px-4 py-2 text-sm font-semibold text-white hover:bg-ember-500">Add cluster</button>}>
      {clusters.length === 0 ? <p className="text-sm text-muted">No backend clusters are configured.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Name</th><th className="pb-3">Backend</th><th className="pb-3">Region</th><th className="pb-3">Health</th><th className="pb-3">Capabilities</th><th className="pb-3">Dependents</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{clusters.map((cluster) => { const view = views[cluster.name]; return <tr key={cluster.name} className="cursor-pointer hover:bg-ink-800/60" onClick={() => setSelected(cluster.name)}><td className="py-3 font-semibold text-ember-300">{cluster.name}</td><td className="py-3">{cluster.type} · {cluster.scheme}</td><td className="py-3 text-muted">{cluster.region}</td><td className={`py-3 ${view?.probe.reachable ? 'text-emerald-300' : 'text-red-300'}`}>{view ? (view.probe.reachable ? `${view.probe.latency_ms.toFixed(1)} ms` : 'unreachable') : 'checking…'}</td><td className="py-3 text-muted">{profile(cluster, view)}</td><td className="py-3">{cluster.references?.length ?? view?.references?.length ?? 0}</td></tr> })}</tbody>
      </table></div>}
    </Card>

    <Drawer open={adding} title="Add a backend cluster" eyebrow="Clusters" onClose={() => setAdding(false)} footer={<div className="grid grid-cols-2 gap-3"><button type="button" disabled={busy || !form.name || !form.endpoint || !form.accessKey || !form.secret} onClick={() => void onProbe()} className="rounded-lg border border-ember-500 px-4 py-2 font-semibold disabled:opacity-50">Run probe</button><button form="cluster-add" type="submit" disabled={busy || !probe || probedInput !== currentInput} className="rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Save cluster</button></div>}>
      <form id="cluster-add" onSubmit={(event) => void onSave(event)} className="grid gap-4">
        <label className="text-sm font-medium">Name<input value={form.name} onChange={(event) => update('name', event.target.value)} className={inputClass} placeholder="vast02" /></label>
        <div className="grid grid-cols-[110px_1fr] gap-3"><label className="text-sm font-medium">Scheme<select value={form.scheme} onChange={(event) => update('scheme', event.target.value)} className={inputClass}><option>http</option><option>https</option></select></label><label className="text-sm font-medium">Endpoint<input value={form.endpoint} onChange={(event) => update('endpoint', event.target.value)} className={inputClass} placeholder="host:443" /></label></div>
        <label className="text-sm font-medium">Region<input value={form.region} onChange={(event) => update('region', event.target.value)} className={inputClass} /></label>
        <label className="text-sm font-medium">Access key<input value={form.accessKey} onChange={(event) => update('accessKey', event.target.value)} className={inputClass} autoComplete="off" /></label>
        <fieldset><legend className="text-sm font-medium">Secret source</legend><div className="mt-2 flex gap-4 text-sm"><label><input type="radio" checked={form.secretMode === 'value'} onChange={() => update('secretMode', 'value')} /> Typed value</label><label><input type="radio" checked={form.secretMode === 'ref'} onChange={() => update('secretMode', 'ref')} /> secret_ref</label></div><input type={form.secretMode === 'value' ? 'password' : 'text'} value={form.secret} onChange={(event) => update('secret', event.target.value)} className={inputClass} placeholder={form.secretMode === 'value' ? 'Secret key' : 'env:NAME or file:/path'} autoComplete="off" /></fieldset>
        {probe && <div className={`rounded-lg border p-4 text-sm ${probe.profile === 'assumed' ? 'border-amber-500/60 bg-amber-950/20' : 'border-emerald-600/60 bg-emerald-950/20'}`}><p className="font-semibold">Credential probe measured: reachable</p><p className="mt-1 text-muted">{probe.cluster.type} · conditional write {String(probe.capabilities.conditional_write.value)} · conditional delete {String(probe.capabilities.conditional_delete.value)}</p>{probe.profile === 'assumed' && <p className="mt-2 text-amber-200">Capability defaults are assumed. Expand will measure conditional behavior against a target bucket.</p>}</div>}
      </form>
    </Drawer>

    <Drawer open={Boolean(selected)} title={selected ?? ''} eyebrow="Cluster detail" onClose={() => { setSelected(null); setHistory([]); setRemoval(null) }}>
      {detail ? <div className="space-y-6">
        <div className="grid grid-cols-2 gap-3 text-sm"><div><p className="text-muted">Endpoint</p><p className="mt-1 font-mono">{detail.scheme}://{detail.endpoints.join(', ')}</p></div><div><p className="text-muted">Health history</p><Sparkline values={history.length ? history : [detail.probe.latency_ms]} label={`${detail.name} latency history`} /></div></div>
        <Card title="Capability profile"><dl className="grid gap-3 text-sm">{Object.entries(detail.capabilities).map(([name, value]) => <div key={name} className="flex justify-between"><dt className="text-muted">{name.replaceAll('_', ' ')}</dt><dd>{String(value.value)} · {value.known ? 'measured' : 'assumed'}</dd></div>)}</dl></Card>
        {detail.secret && <Card title="Secret" eyebrow={`generation ${detail.secret.generation}`}>
          <p className="text-sm">{detail.secret.pending.length === 0 && detail.secret.silent.length === 0 ? <span className="text-emerald-300">Every proxy signs with this secret.</span> : <span className="text-amber-200">Rotation installing: {detail.secret.pending.length} {detail.secret.pending.length === 1 ? 'proxy' : 'proxies'} still on an older secret{detail.secret.silent.length ? `, ${detail.secret.silent.length} silent` : ''}.</span>}</p>
          <dl className="mt-3 grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs"><dt className="text-muted">Installed</dt><dd className="font-mono">{detail.secret.installed.join(', ') || '—'}</dd><dt className="text-muted">Pending</dt><dd className="font-mono">{detail.secret.pending.join(', ') || '—'}</dd><dt className="text-muted">Silent</dt><dd className="font-mono">{detail.secret.silent.join(', ') || '—'}</dd></dl>
          <p className="mt-3 text-xs text-muted">Installed is not drained: a request a proxy began before it installed the new secret may still sign with the old one. Keep the old backend key valid until requests that old have finished.</p>
        </Card>}
        <Card title="Dependent placements">{detail.references?.length ? <ul className="grid gap-2 text-sm">{detail.references.map((ref) => {
          // A tenant defaulting to this cluster blocks its removal; the fix is here, not on another screen.
          const tenant = /^tenants\.(.+)\.default_cluster$/.exec(ref)?.[1]
          const others = clusters.filter((c) => c.name !== detail.name)
          if (!tenant) return <li key={ref} className="font-mono">{ref}</li>
          const choice = newDefault[tenant] ?? others[0]?.name ?? ''
          return <li key={ref}><span className="font-mono">{ref}</span><span className="mt-1 block text-xs text-muted">New buckets of tenant {tenant} are created on {detail.name}.</span>{others.length > 0 && <div className="mt-2 flex flex-wrap items-center gap-2"><select aria-label={`New default cluster for tenant ${tenant}`} value={choice} onChange={(event) => setNewDefault((old) => ({ ...old, [tenant]: event.target.value }))} className="rounded-lg border border-ink-700 bg-ink-950 px-3 py-1.5 text-sm">{others.map((c) => <option key={c.name}>{c.name}</option>)}</select><button type="button" disabled={busy || !choice} onClick={() => void act(() => setTenantDefault(token, tenant, choice), `${choice} is now tenant ${tenant}'s default cluster`).then((ok) => { if (ok) setRemoval(null) })} className="rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold">Make it tenant {tenant}'s default</button></div>}</li>
        })}</ul> : <p className="text-sm text-muted">Nothing references this cluster.</p>}</Card>
        <div className="flex flex-wrap gap-3"><button type="button" disabled={busy} onClick={() => void act(() => setClusterReadOnly(token, detail.name, !detail.read_only), `Cluster ${detail.name} is ${detail.read_only ? 'writable' : 'read-only'}`)} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold">Make {detail.read_only ? 'writable' : 'read-only'}</button><button type="button" disabled={busy} onClick={() => { setBusy(true); void removeClusterDryRun(token, detail.name).then(setRemoval).catch((error) => notify(error instanceof Error ? error.message : String(error), 'danger')).finally(() => setBusy(false)) }} className="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white">Remove cluster</button></div>
        {removal && <div className="rounded-lg border border-red-600/60 bg-red-950/20 p-4 text-sm"><p className="font-semibold">Dry run</p><p className="mt-2 text-red-100">{removal.allowed ? `Will remove ${removal.name} and ${removal.secret_files} stored secret file(s).` : removal.reason}</p>{removal.allowed && removal.token && <button type="button" className="mt-4 rounded-lg bg-red-600 px-4 py-2 font-semibold text-white" disabled={busy} onClick={() => void act(() => removeCluster(token, detail.name, removal.token ?? ''), `Cluster ${detail.name} removed`).then((ok) => { if (ok) { setSelected(null); setRemoval(null) } })}>Confirm removal</button>}</div>}
      </div> : <p className="text-muted">Loading cluster detail…</p>}
    </Drawer>
  </div>
}
