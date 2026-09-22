import { useState } from 'react'
import { Card } from './components/Card'
import { CopyLine } from './components/CopyLine'
import { Stat } from './components/Stat'
import { useStore } from './store'

const pages = ['Control plane', 'Clusters', 'Buckets', 'Migrations', 'Telemetry', 'Audit'] as const

function TokenPrompt() {
  const { setToken } = useStore()
  const [value, setValue] = useState('')
  return <main className="grid min-h-screen place-items-center bg-[radial-gradient(circle_at_70%_20%,rgba(204,85,0,.16),transparent_34%)] p-6">
    <Card eyebrow="shunt control" title="Connect to this control node" className="w-full max-w-md">
      <form onSubmit={(event) => { event.preventDefault(); setToken(value) }}>
        <label htmlFor="token" className="text-sm font-medium">Bearer token</label>
        <p className="mt-1 text-sm text-muted">Stored only in this browser tab. It is never put in a URL.</p>
        <input id="token" type="password" autoComplete="off" value={value} onChange={(event) => setValue(event.target.value)} className="mt-4 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper placeholder:text-muted" placeholder="Paste token" />
        <button type="submit" disabled={!value.trim()} className="mt-4 w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white hover:bg-ember-500 disabled:cursor-not-allowed disabled:opacity-50">Connect</button>
      </form>
    </Card>
  </main>
}

function ControlPlane() {
  const { control, fleet, loading, error, refresh } = useStore()
  if (!control) return <Card title="Control plane"><p role={error ? 'alert' : 'status'} className="text-muted">{error || (loading ? 'Loading control state…' : 'No control state yet.')}</p></Card>
  const members = fleet?.members ?? control.fleet ?? []
  const stale = members.filter((member) => !member.live)
  return <div className="space-y-5">
    {error && <div role="alert" className="rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">{error}</div>}
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Card><Stat label="Quorum" value={control.cluster.has_quorum ? 'Healthy' : 'Lost'} detail={`${control.cluster.started} started · ${control.cluster.quorum} needed`} /></Card>
      <Card><Stat label="Fleet" value={members.length} detail={`${members.filter((member) => member.live).length} live`} /></Card>
      <Card><Stat label="Directory" value={control.directory} detail={`etcd revision ${control.cluster.revision}`} /></Card>
      <Card><Stat label="Database" value={`${(control.cluster.db_in_use_bytes / 1048576).toFixed(1)} MiB`} detail={`${(control.cluster.db_bytes / 1048576).toFixed(1)} MiB allocated`} /></Card>
    </div>
    <Card eyebrow="Consensus" title="Control members" action={<button type="button" onClick={() => void refresh()} className="rounded-md border border-ink-700 px-3 py-1.5 text-xs font-semibold text-muted hover:border-ember-500 hover:text-paper">Refresh</button>}>
      <div className="divide-y divide-ink-700">
        {control.cluster.members.map((member) => <div key={member.id} className="grid gap-2 py-3 text-sm sm:grid-cols-[1fr_auto_auto] sm:items-center">
          <div><span className="font-semibold">{member.name}</span><span className="ml-2 text-muted">{member.peer_urls[0]}</span></div>
          <span className="text-muted">{member.leader ? 'leader' : member.learner ? 'learner' : 'follower'}</span>
          <span className={member.started ? 'text-emerald-300' : 'text-red-300'}>{member.started ? 'started' : 'not started'}</span>
        </div>)}
      </div>
    </Card>
    <Card eyebrow="Data plane" title="Proxy fleet">
      {members.length === 0 ? <p className="text-sm text-muted">No proxies have joined.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Proxy</th><th className="pb-3">Host</th><th className="pb-3">Applied</th><th className="pb-3">State</th><th className="pb-3">Version</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{members.map((member) => <tr key={member.id}><td className="py-3 font-medium">{member.id}</td><td className="py-3 text-muted">{member.host || '—'}</td><td className="py-3 font-mono">{member.applied} / {fleet?.version ?? control.directory}</td><td className={`py-3 ${member.live ? 'text-emerald-300' : 'text-red-300'}`}>{member.live ? 'live' : 'stale'}</td><td className="py-3 text-muted">{member.version || '—'}</td></tr>)}</tbody>
      </table></div>}
      {stale.length > 0 && <p className="mt-4 rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">Stale proxies: {stale.map((member) => member.id).join(', ')}</p>}
    </Card>
    <Card eyebrow="Add node" title="Join another control member"><CopyLine value={control.join} /></Card>
  </div>
}

export function App() {
  const store = useStore()
  const [page, setPage] = useState<(typeof pages)[number]>('Control plane')
  if (!store.token) return <TokenPrompt />
  const members = store.fleet?.members ?? store.control?.fleet ?? []
  const stale = members.filter((member) => !member.live).length
  return <div className="min-h-screen bg-ink-950 lg:grid lg:grid-cols-[240px_1fr]">
    <aside className="border-b border-ink-700 bg-ink-900 p-5 lg:min-h-screen lg:border-b-0 lg:border-r">
      <div className="mb-8"><p className="text-xl font-semibold tracking-tight">shunt</p><p className="eyebrow mt-1">control surface</p></div>
      <nav aria-label="Primary" className="grid grid-cols-2 gap-1 sm:grid-cols-3 lg:grid-cols-1">
        {pages.map((name) => <button key={name} type="button" aria-current={page === name ? 'page' : undefined} onClick={() => setPage(name)} className={`rounded-lg px-3 py-2 text-left text-sm ${page === name ? 'bg-ember-600 text-white' : 'text-muted hover:bg-ink-800 hover:text-paper'}`}>{name}</button>)}
      </nav>
    </aside>
    <div className="min-w-0">
      <header className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-3 border-b border-ink-700 bg-ink-950/95 px-5 py-3 backdrop-blur">
        <div><p className="text-sm font-semibold">{store.control?.node ?? 'Connecting…'}</p><p className="text-xs text-muted">{store.control?.cluster.has_quorum ? 'quorum healthy' : 'quorum unavailable'} · {members.length} proxies</p></div>
        <div className="flex items-center gap-3">{stale > 0 && <span className="rounded-full border border-red-600/60 bg-red-950/40 px-2.5 py-1 text-xs text-red-200">{stale} stale</span>}<button type="button" onClick={store.clearToken} className="rounded-md border border-ink-700 px-3 py-1.5 text-xs text-muted hover:text-paper">Forget token</button></div>
      </header>
      <main className="p-5 sm:p-7"><div className="mb-6"><p className="eyebrow">Live control data</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">{page}</h1></div>
        {page === 'Control plane' ? <ControlPlane /> : <Card title={page}><p className="text-muted">This screen arrives in the next UI phase. The live shell, authentication, and event stream are already connected.</p></Card>}
      </main>
    </div>
    <div aria-live="polite" aria-atomic="true" className="fixed bottom-4 right-4 z-50 grid max-w-sm gap-2">{store.toasts.map((toast) => <button type="button" key={toast.id} onClick={() => store.dismissToast(toast.id)} className={`rounded-lg border p-3 text-left text-sm shadow-xl ${toast.tone === 'danger' ? 'border-red-600 bg-red-950 text-red-100' : 'border-ember-500 bg-ink-900 text-paper'}`}>{toast.message}</button>)}</div>
  </div>
}
