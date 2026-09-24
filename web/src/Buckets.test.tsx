import { fireEvent, render, screen } from '@testing-library/react'
import { App } from './App'
import { ErrorBoundary } from './components/ErrorBoundary'
import { StoreProvider } from './store'

const control = { node: 'c1', version: 't', directory: 3, directory_loaded: true, last_compaction: null, join: '', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: [], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 3, db_bytes: 1, db_in_use_bytes: 1, quota_bytes: 2, leader: 'c1' } }

// A bucket spread over two legs (ADR-0018) has no single primary: its names are null and its
// buckets are its legs.
const spread = {
  key: 'default/data01', state: 'ACTIVE', primary: '', names: null, ramp_writes: {}, fallback_reads: 0, dual_deletes: {}, read_only: false, reject_writes: false, client_keys: 1, per_bucket_telemetry: true,
  legs: [
    { id: 'minio-b', cluster: 'minio-b', bucket: 'data01', share: 0.5, ranges: [{ from: '0000000000000000', to: '7ffffffffffffffe' }] },
    { id: 'minio-a', cluster: 'minio-a', bucket: 'data01', share: 0.5, ranges: [{ from: '7fffffffffffffff', to: 'ffffffffffffffff' }] },
  ],
}
const single = { key: 'default/ui-demo', state: 'ACTIVE', primary: 'minio-b', names: { 'minio-b': 'ui-demo' }, ramp_writes: {}, fallback_reads: 0, dual_deletes: {}, read_only: false, reject_writes: false, client_keys: 1 }

afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

test('opens the detail of a bucket spread over legs, which has no single set of names', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 5, clusters: [], placements: [spread, single] })
    if (url.includes('/data01/view')) return Response.json({ ...spread, fence: { version: 5, held: false, proxies: 2, waiting_on: [], silent: [] }, operations: [], clusters: {} })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByText('data01'))
  expect(await screen.findByText('Legs')).toBeInTheDocument()
  expect(screen.queryByText('Placement names')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Buckets' })).toBeInTheDocument() // the app is still there
  expect(screen.getByText(/counted without a watch/)).toBeInTheDocument() // a spread bucket is counted by backend anyway
})

test('watches a bucket from its detail', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  const posted: string[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    if (init?.method === 'POST') { posted.push(`${url} ${String(init.body)}`); return Response.json({ key: 'default/ui-demo', watch: true, version: 6 }) }
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 5, clusters: [], placements: [single] })
    if (url.includes('/ui-demo/view')) return Response.json({ ...single, fence: { version: 5, held: false, proxies: 2, waiting_on: [], silent: [] }, operations: [], clusters: {} })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Buckets' }))
  fireEvent.click((await screen.findAllByText('ui-demo'))[0])
  fireEvent.click(await screen.findByRole('button', { name: 'Watch traffic by backend' }))
  await screen.findByText(/its traffic by backend shows on Telemetry/)
  expect(posted).toEqual(['/v1/placements/default/ui-demo/watch {"watch":true}'])
})

function Broken(): never {
  throw new Error('render exploded')
}

test('keeps a screen that fails to render to that screen, and forgets it on another screen', () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const { rerender } = render(<ErrorBoundary resetKey="Buckets"><Broken /></ErrorBoundary>)
  expect(screen.getByRole('alert')).toHaveTextContent('This screen failed to render: render exploded')
  rerender(<ErrorBoundary resetKey="Clusters"><p>clusters screen</p></ErrorBoundary>)
  expect(screen.getByText('clusters screen')).toBeInTheDocument()
})
