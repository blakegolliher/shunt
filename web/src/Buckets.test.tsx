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

// With nothing migrating, Migrations lists the buckets and what would start a migration of each;
// the button opens that drawer on Buckets.
test('lists the buckets on an idle Migrations screen and opens the matching drawer', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  const clusters = ['minio-a', 'minio-b'].map((name) => ({ name, type: 'minio', scheme: 'http', region: 'us-east-1', endpoints: [`${name}:9000`], access_key: 'AK', secret_ref: `control:${name}`,
    conditional_write: true, conditional_delete: false, references: [], read_only: false, reject_writes: false }))
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 5, clusters, placements: [spread, single] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Migrations' }))
  expect(await screen.findByText('No bucket is migrating')).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Move keys of default/data01' })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Consolidate default/data01' })).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Expand default/ui-demo' }))
  expect(await screen.findByText('Expand default/ui-demo')).toBeInTheDocument()
})

// Moving part of a spread bucket to a cluster it has no leg on (ADR-0018 N3): the drawer starts on
// that cluster and asks for the new leg's bucket, saying it is created if missing and must be empty
// if not; the move names its leg and share, which the control plane turns into the hash range, and
// the drawer shows the same move as a `shunt ramp` line. From Adopt or create, naming the spread
// bucket offers Move keys to the cluster picked there, with the bucket name typed there.
function spreadMock(onCall?: (url: string, init?: RequestInit) => Response | undefined) {
  sessionStorage.setItem('shunt.control.token', 't')
  const cluster = (name: string, type = 'minio') => ({ name, type, scheme: 'http', region: 'us-east-1', endpoints: [`${name}:9000`], access_key: 'AK', secret_ref: `control:${name}`,
    conditional_write: true, conditional_delete: false, references: [], read_only: false, reject_writes: false })
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    const answered = onCall?.(url, init)
    if (answered) return answered
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 5, clusters: [cluster('minio-a'), cluster('minio-b'), cluster('vast-c', 'vast')], placements: [spread, single] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
}

test('moves half of a leg to a cluster the bucket has no leg on, naming the new leg\'s bucket', async () => {
  let sent: { kind?: string; placement?: string; args?: Record<string, unknown> } = {}
  spreadMock((url, init) => {
    if (url.endsWith('/v1/operations') && init?.method === 'POST') {
      sent = JSON.parse(String(init.body)) as typeof sent
      return Response.json({ id: 'op-move', kind: 'ramp', placement: 'default/data01', actor: 'token:t', status: 'succeeded', phase: 'done', version: 6 })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Move keys of default/data01' }))
  expect(screen.getByLabelText('To cluster')).toHaveValue('vast-c')
  expect(screen.getByLabelText(/New leg's bucket name on vast-c/)).toHaveValue('')
  expect(screen.getByText(/The first step creates it on vast-c if it does not exist/)).toHaveTextContent('must be empty, or the move is refused')
  expect(screen.getByText('shunt ramp default/data01 --leg minio-b --share 0.5 --to vast-c --name data01 --create --ratio 0.01')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Start the move and continue to Migrations' }))
  await vi.waitFor(() => expect(sent.kind).toBe('ramp'))
  expect(sent.placement).toBe('default/data01')
  expect(sent.args).toEqual({ to: 'vast-c', name: 'data01', leg: 'minio-b', share: 0.5, ratio: 0.01, create: true, wait: '30s' })
})

test('from Adopt or create, a spread bucket offers to expand to the cluster picked there', async () => {
  spreadMock()
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Adopt or create' }))
  fireEvent.change(screen.getByLabelText('Client bucket'), { target: { value: 'data01' } })
  fireEvent.change(screen.getByLabelText(/^Cluster/), { target: { value: 'vast-c' } })
  fireEvent.change(screen.getByLabelText(/Backend bucket name/), { target: { value: 'data01-v' } })
  fireEvent.click(await screen.findByRole('button', { name: 'Expand data01 to vast-c' }))
  expect(await screen.findByText('Expand default/data01 to another cluster')).toBeInTheDocument()
  expect(screen.getByLabelText('To cluster')).toHaveValue('vast-c')
  expect(screen.getByLabelText(/New leg's bucket name on vast-c/)).toHaveValue('data01-v')
  expect(screen.getByText(/--to vast-c --name data01-v --create/)).toBeInTheDocument()
})

// A spread bucket has Expand too, as a plain one does (found in a manual pass, 2026-09-29: the
// operator looked for Expand, and a spread bucket offered only Move keys): on its row and on an idle
// Migrations screen it opens the move drawer as expanding the bucket, on the cluster it has no leg on.
test('expands a spread bucket from its row and from Migrations', async () => {
  spreadMock()
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Expand default/data01' }))
  expect(await screen.findByText('Expand default/data01 to another cluster')).toBeInTheDocument()
  expect(screen.getByText(/data01 is spread over 2 backend buckets, so it expands by a move/)).toBeInTheDocument()
  expect(screen.getByLabelText('To cluster')).toHaveValue('vast-c')
  fireEvent.click(screen.getByRole('button', { name: 'Close' }))
  fireEvent.click(screen.getByRole('button', { name: 'Migrations' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Expand default/data01' }))
  expect(await screen.findByText('Expand default/data01 to another cluster')).toBeInTheDocument()
  expect(screen.getByLabelText('To cluster')).toHaveValue('vast-c')
})

