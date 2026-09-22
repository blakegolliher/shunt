import { useMemo, useState } from 'react'
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

export function ControlPlane() {
  const { control, fleet, loading, error, refresh } = useStore()
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
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Proxy</th><th className="pb-3">Host</th><th className="pb-3">Applied</th><th className="pb-3">State</th><th className="pb-3">Last heartbeat</th><th className="pb-3">Version</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{members.map((member) => <tr key={member.id}><td className="py-3 font-medium">{member.id}</td><td className="py-3 text-muted">{member.host || '—'}</td><td className="py-3 font-mono">{member.applied} / {fleet?.version ?? control.directory}</td><td className={`py-3 ${member.live ? 'text-emerald-300' : 'text-red-300'}`}>{member.live ? 'live' : 'stale'}</td><td className="py-3 text-muted">{member.seen ? new Date(member.seen).toLocaleTimeString() : '—'}</td><td className="py-3 text-muted">{member.version || '—'}</td></tr>)}</tbody>
      </table></div>}
      {stale.length > 0 && <p className="mt-4 rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">Stale proxies: {stale.map((member) => member.id).join(', ')}</p>}
    </Card>
    <Drawer open={adding} title="Join a control member" eyebrow="Control plane" onClose={() => setAdding(false)} footer={<button type="button" disabled={!name.trim() || !peerURL.trim()} onClick={() => { setExpected(name.trim()); setAdding(false) }} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">I ran this command</button>}>
      <div className="grid gap-4"><label className="text-sm font-medium">New member name<input value={name} onChange={(event) => setName(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label><label className="text-sm font-medium">Peer URL<input value={peerURL} onChange={(event) => setPeerURL(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label><p className="text-sm text-muted">Run this exact command on the new host. This member will be highlighted when it appears.</p><CopyLine value={join} /></div>
    </Drawer>
  </div>
}
