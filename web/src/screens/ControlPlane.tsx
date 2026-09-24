import { useEffect, useMemo, useState } from 'react'
import { forgetProxy, getProxyDiagnostics, resolveProxy, retireProxy } from '../api/client'
import type { ProxyDiagnostics, ProxyMember } from '../api/client'
import { Card } from '../components/Card'
import { CopyLine } from '../components/CopyLine'
import { Drawer } from '../components/Drawer'
import { Stat } from '../components/Stat'
import { useStore } from '../store'

function bytes(value: number) {
  if (!value) return '0 B'
  if (value < 1048576) return `${(value / 1024).toFixed(1)} KiB`
  return `${(value / 1048576).toFixed(1)} MiB`
}

// memberState is one word for where a proxy stands (ADR-0021 D2): a process that did not retire
// cleanly blocks every barrier until an operator resolves it, which comes before live or silent.
function memberState(m: ProxyMember): { text: string; tone: string } {
  if (m.unresolved?.length) return { text: `unresolved (${m.unresolved.length})`, tone: 'text-red-300' }
  if (m.incarnation?.state === 'retired') return { text: 'retired', tone: 'text-muted' }
  if (!m.live) return { text: 'silent', tone: 'text-red-300' }
  if (m.retire_requested) return { text: 'retiring', tone: 'text-amber-200' }
  return { text: 'live', tone: 'text-emerald-300' }
}

export function ControlPlane() {
  const { control, fleet, loading, error, refresh, token, notify } = useStore()
  const [shown, setShown] = useState<string | null>(null) // the proxy whose install diagnostics the drawer shows
  const [diagnostics, setDiagnostics] = useState<ProxyDiagnostics | null>(null)
  const [diagnosticsError, setDiagnosticsError] = useState('')
  const [attestation, setAttestation] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => {
    if (!shown) return
    let live = true
    getProxyDiagnostics(token, shown).then((d) => { if (live) { setDiagnostics(d); setDiagnosticsError('') } }).catch((err: unknown) => { if (live) setDiagnosticsError(err instanceof Error ? err.message : String(err)) })
    return () => { live = false }
  }, [shown, token, fleet])
  const act = async (fn: () => Promise<unknown>, success: string) => {
    setBusy(true)
    try { await fn(); notify(success); await refresh(); return true } catch (err) { notify(err instanceof Error ? err.message : String(err), 'danger'); return false } finally { setBusy(false) }
  }
  const [adding, setAdding] = useState(false)
  const [name, setName] = useState('')
  const [peerURL, setPeerURL] = useState('http://host:2380')
  const [expected, setExpected] = useState('')
  const join = useMemo(() => {
    if (!control) return ''
    let line = control.join.replace('<name>', name || '<name>')
    if (peerURL) {
      line = line.replace('http://<host>:2380', peerURL)
      try { line = line.replaceAll('<host>', new URL(peerURL).hostname) } catch { /* keep the server placeholder */ }
    }
    return line
  }, [control, name, peerURL])

  if (!control) return <Card title="Control plane"><p role={error ? 'alert' : 'status'} className="text-muted">{error || (loading ? 'Loading control state…' : 'No control state yet.')}</p></Card>
  const members = fleet?.members ?? control.fleet ?? []
  const stale = members.filter((member) => !member.live)
  const quorumMath = `${control.cluster.started} started ≥ ${control.cluster.quorum} required of ${control.cluster.members.length}`
  return <div className="space-y-5">
    {error && <div role="alert" className="rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">{error}</div>}
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Card><Stat label="Quorum" value={control.cluster.has_quorum ? 'Healthy' : 'Lost'} detail={quorumMath} /></Card>
      <Card><Stat label="Fleet" value={members.length} detail={`${members.filter((member) => member.live).length} live`} /></Card>
      <Card><Stat label="Directory" value={control.directory} detail={`etcd revision ${control.cluster.revision}`} /></Card>
      <Card><Stat label="Database" value={bytes(control.cluster.db_in_use_bytes)} detail={`${bytes(control.cluster.db_bytes)} / ${bytes(control.cluster.quota_bytes)} quota`} /></Card>
    </div>
    <Card eyebrow="Consensus" title="Control members" action={<div className="flex gap-2"><button type="button" onClick={() => setAdding(true)} className="rounded-md bg-ember-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-ember-500">Add node</button><button type="button" onClick={() => void refresh()} className="rounded-md border border-ink-700 px-3 py-1.5 text-xs font-semibold text-muted hover:border-ember-500 hover:text-paper">Refresh</button></div>}>
      <p className="mb-3 text-sm text-muted">Leader: <span className="text-paper">{control.cluster.leader || 'none'}</span> · Majority: {quorumMath}</p>
      <div className="divide-y divide-ink-700">
        {control.cluster.members.map((member) => <div key={member.id} data-highlighted={member.name === expected || undefined} className={`grid gap-2 rounded-lg px-2 py-3 text-sm sm:grid-cols-[1fr_auto_auto] sm:items-center ${member.name === expected ? 'bg-ember-600/20 ring-1 ring-ember-400' : ''}`}>
          <div><span className="font-semibold">{member.name || 'joining…'}</span><span className="ml-2 text-muted">{member.peer_urls[0]}</span></div>
          <span className="text-muted">{member.leader ? 'leader' : member.learner ? 'learner' : 'follower'}</span>
          <span className={member.started ? 'text-emerald-300' : 'text-red-300'}>{member.started ? 'healthy' : 'not started'}</span>
        </div>)}
      </div>
    </Card>
    <Card eyebrow="Data plane" title="Proxy fleet">
      {members.length === 0 ? <p className="text-sm text-muted">No proxies have joined.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Proxy</th><th className="pb-3">Host</th><th className="pb-3">Applied</th><th className="pb-3">Durable</th><th className="pb-3">State</th><th className="pb-3">Last heartbeat</th><th className="pb-3">Version</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{members.map((member) => { const state = memberState(member); return <tr key={member.id} className="cursor-pointer hover:bg-ink-800/60" onClick={() => setShown(member.id)}><td className="py-3 font-medium text-ember-300">{member.id}</td><td className="py-3 text-muted">{member.host || '—'}</td><td className="py-3 font-mono">{member.applied} / {fleet?.version ?? control.directory}</td><td className={`py-3 font-mono ${(member.durable ?? 0) < member.applied ? 'text-amber-200' : ''}`}>{member.durable ?? '—'}{(member.durable ?? 0) < member.applied ? ' (not durable)' : ''}</td><td className={`py-3 ${state.tone}`}>{state.text}</td><td className="py-3 text-muted">{member.seen ? new Date(member.seen).toLocaleTimeString() : '—'}</td><td className="py-3 text-muted">{member.version || '—'}</td></tr> })}</tbody>
      </table></div>}
      {stale.length > 0 && <p className="mt-4 rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">Silent proxies: {stale.map((member) => member.id).join(', ')}. A silent proxy's process may still be running: every barrier waits on it until it is back, retires, or an operator resolves its incarnation.</p>}
    </Card>
    <Drawer open={Boolean(shown)} title={shown ?? ''} eyebrow="Proxy install" onClose={() => { setShown(null); setDiagnostics(null); setDiagnosticsError('') }}>
      {diagnosticsError ? <p role="alert" className="text-red-300">{diagnosticsError}</p> : diagnostics && diagnostics.id === shown ? <div className="space-y-5">
        <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-2 text-sm">
          <dt className="text-muted">Directory</dt><dd className="font-mono">{diagnostics.directory}</dd>
          <dt className="text-muted">Requests use</dt><dd className="font-mono">{diagnostics.applied}{diagnostics.lineage ? '' : ' (other lineage)'}</dd>
          <dt className="text-muted">Installed</dt><dd className="font-mono">{Math.max(diagnostics.installed ?? 0, diagnostics.applied)}</dd>
          <dt className="text-muted">Restart cache</dt><dd className="font-mono">{diagnostics.durable ?? 0}</dd>
        </dl>
        {diagnostics.secrets.length > 0 && <Card title="Cluster secrets"><dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">{diagnostics.secrets.map((sec) => <div key={sec.cluster} className="contents"><dt className="text-muted">{sec.cluster}</dt><dd className={`font-mono ${sec.current ? '' : 'text-amber-200'}`}>generation {sec.have || 'none'}{sec.current ? '' : ` (control holds ${sec.want})`}</dd></div>)}</dl></Card>}
        {diagnostics.problems.length === 0 ? <p className="text-sm text-emerald-300">Nothing is off with this proxy's install.</p> : <ul className="grid gap-2 text-sm text-amber-200">{diagnostics.problems.map((problem) => <li key={problem}>{problem}</li>)}</ul>}
        <Card title="Incarnation" eyebrow={diagnostics.incarnation ? `${diagnostics.incarnation.state}${diagnostics.retire_requested ? ', retirement requested' : ''}` : 'none running'}>
          {diagnostics.incarnation ? <p className="font-mono text-xs">{diagnostics.incarnation.id}<span className="ml-2 text-muted">since {diagnostics.incarnation.started ? new Date(diagnostics.incarnation.started).toLocaleString() : '—'}{diagnostics.incarnation.ended ? `, ended ${new Date(diagnostics.incarnation.ended).toLocaleString()}` : ''}; {diagnostics.uncertain ?? 0} backend outcomes unknown</span></p> : <p className="text-sm text-muted">No process of this proxy is running or retired; its last one was resolved.</p>}
          {(diagnostics.unresolved ?? []).map((inc) => <div key={inc.id} className="mt-3 rounded-lg border border-red-600/60 bg-red-950/30 p-3 text-sm">
            <p className="text-red-100">Incarnation <span className="font-mono text-xs">{inc.id}</span> did not retire cleanly ({inc.uncertain ?? 0} outcomes unknown{inc.ended ? `, ended ${new Date(inc.ended).toLocaleString()}` : ''}). Every barrier waits on it until you record that the process is stopped and its backend work has ended.</p>
            <label className="mt-2 block text-xs text-muted">Attestation: how that was established<input aria-label={`Attestation for ${inc.id}`} value={attestation} onChange={(event) => setAttestation(event.target.value)} className="mt-1 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper" placeholder="host decommissioned; backend shows no request from it" /></label>
            <button type="button" disabled={busy || !attestation.trim()} onClick={() => void act(() => resolveProxy(token, diagnostics.id, inc.id, attestation.trim()), `Incarnation ${inc.id} of ${diagnostics.id} resolved`).then((ok) => { if (ok) setAttestation('') })} className="mt-2 rounded-lg border border-red-500 px-3 py-1.5 text-xs font-semibold text-red-100 disabled:opacity-50">Resolve incarnation</button>
          </div>)}
          <div className="mt-4 flex flex-wrap gap-3">
            {diagnostics.incarnation?.state === 'active' && diagnostics.live && <button type="button" disabled={busy || diagnostics.retire_requested} onClick={() => void act(() => retireProxy(token, diagnostics.id), `${diagnostics.id}: retirement requested; it drains and stops at its next heartbeat`)} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold disabled:opacity-50">Retire proxy</button>}
            {!diagnostics.live && <button type="button" disabled={busy} onClick={() => void act(() => forgetProxy(token, diagnostics.id), `${diagnostics.id} forgotten`).then((ok) => { if (ok) setShown(null) })} className="rounded-lg border border-ink-600 px-4 py-2 text-sm font-semibold text-muted hover:text-paper disabled:opacity-50">Forget proxy</button>}
          </div>
        </Card>
      </div> : <p className="text-muted">Loading proxy install…</p>}
    </Drawer>
    <Drawer open={adding} title="Join a control member" eyebrow="Control plane" onClose={() => setAdding(false)} footer={<button type="button" disabled={!name.trim() || !peerURL.trim()} onClick={() => { setExpected(name.trim()); setAdding(false) }} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">I ran this command</button>}>
      <div className="grid gap-4"><label className="text-sm font-medium">New member name<input value={name} onChange={(event) => setName(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label><label className="text-sm font-medium">Peer URL<input value={peerURL} onChange={(event) => setPeerURL(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label><p className="text-sm text-muted">Run this exact command on the new host. This member will be highlighted when it appears.</p><CopyLine value={join} /></div>
    </Drawer>
  </div>
}
