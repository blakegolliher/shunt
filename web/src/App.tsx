import { useState } from 'react'
import { Card } from './components/Card'
import { Buckets } from './screens/Buckets'
import { Clusters } from './screens/Clusters'
import { ControlPlane } from './screens/ControlPlane'
import { Migrations } from './screens/Migrations'
import { Telemetry } from './screens/Telemetry'
import { Audit } from './screens/Audit'
import { Operations } from './screens/Operations'
import { useStore } from './store'

const pages = ['Control plane', 'Clusters', 'Buckets', 'Migrations', 'Operations', 'Telemetry', 'Audit'] as const
type Page = (typeof pages)[number]

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

export function App() {
  const store = useStore()
  const [page, setPage] = useState<Page>('Control plane')
  const [migration, setMigration] = useState('')
  if (!store.token) return <TokenPrompt />
  const members = store.fleet?.members ?? store.control?.fleet ?? []
  const stale = members.filter((member) => !member.live).length
  const screen = page === 'Control plane' ? <ControlPlane />
    : page === 'Clusters' ? <Clusters />
      : page === 'Buckets' ? <Buckets onMigrate={(key) => { setMigration(key); setPage('Migrations') }} />
        : page === 'Migrations' ? <Migrations selected={migration} onSelect={setMigration} />
          : page === 'Operations' ? <Operations />
          : page === 'Telemetry' ? <Telemetry />
            : <Audit />
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
      <main className="p-5 sm:p-7"><div className="mb-6"><p className="eyebrow">Live control data</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">{page}</h1></div>{store.error && page !== 'Control plane' && <div role="alert" className="mb-5 rounded-lg border border-red-600 bg-red-950/40 p-3 text-sm text-red-100">Control data unavailable: {store.error}. Existing data may be stale.</div>}{store.control && !store.control.cluster.has_quorum && <div role="alert" className="mb-5 rounded-lg border border-red-600 bg-red-950/40 p-3 text-sm text-red-100">Control-plane quorum is lost. Read models may be stale and mutating actions are unavailable until a majority returns.</div>}{screen}</main>
    </div>
    <div aria-live="polite" aria-atomic="true" className="fixed bottom-4 left-4 z-50 grid max-w-sm gap-2">{store.toasts.map((toast) => <button type="button" key={toast.id} onClick={() => store.dismissToast(toast.id)} className={`rounded-lg border p-3 text-left text-sm shadow-xl ${toast.tone === 'danger' ? 'border-red-600 bg-red-950 text-red-100' : 'border-ember-500 bg-ink-900 text-paper'}`}>{toast.message}</button>)}</div>
  </div>
}
