import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { App } from './App'
import type { ClusterStatus, DirectoryStatus, PlacementStatus } from './api/client'
import { StoreProvider } from './store'

const source: ClusterStatus = { name: 'source', type: 's3', scheme: 'http', region: 'us-east-1', endpoints: ['source:9000'], access_key: 'AK', secret_ref: 'control:source', conditional_write: true, conditional_delete: false, references: [], read_only: false, reject_writes: false }
const target: ClusterStatus = { ...source, name: 'target', endpoints: ['target:9000'], secret_ref: 'control:target' }
const control = {
  node: 'c1', version: 'test', directory: 3, directory_loaded: true, last_compaction: null,
  join: 'shunt-control join --name <name> --data-dir <data-dir> --peer-url http://<host>:2380 --api <host>:9901 --existing http://c1:9901', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: ['http://c1:2380'], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 3, db_bytes: 1024, db_in_use_bytes: 512, quota_bytes: 2048, leader: 'c1' },
}

function baseMock(directory: DirectoryStatus, mutate?: (url: string, init?: RequestInit) => Response | undefined) {
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    const changed = mutate?.(url, init)
    if (changed) return changed
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: directory.version, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json(directory)
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    if (url.includes('/v1/clusters/') && url.endsWith('/view')) {
      const cluster = directory.clusters.find((item) => url.includes(`/clusters/${item.name}/`)) ?? source
      return Response.json({ ...cluster, capabilities: { conditional_write: { value: true, known: false }, conditional_delete: { value: false, known: false } }, probe: { reachable: true, latency_ms: 2, checked_at: '2026-09-22T12:00:00Z' } })
    }
    return Response.json({ message: `unhandled ${init?.method ?? 'GET'} ${url}` }, { status: 404 })
  })
}

beforeEach(() => sessionStorage.setItem('shunt.control.token', 'actor-token'))
afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

test('renders a filled join command and highlights the expected member', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [], placements: [] }
  baseMock(directory)
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Add node' }))
  fireEvent.change(screen.getByLabelText('New member name'), { target: { value: 'c2' } })
  fireEvent.change(screen.getByLabelText('Peer URL'), { target: { value: 'http://node2:2380' } })
  expect(screen.getByText(/join --name c2/)).toHaveTextContent('--peer-url http://node2:2380 --api node2:9901')
  fireEvent.click(screen.getByRole('button', { name: 'I ran this command' }))
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
})

test('requires a successful unchanged probe before saving a cluster', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [], placements: [] }
  const fetchMock = baseMock(directory, (url, init) => {
    if (url.endsWith('/clusters/probe')) return Response.json({ cluster: target, reachable: true, profile: 'assumed', capabilities: { conditional_write: { value: true, known: false }, conditional_delete: { value: false, known: false } } })
    if (url.endsWith('/v1/clusters') && init?.method === 'POST') { directory.clusters = [target]; directory.version++; return Response.json(target) }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Clusters' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Add cluster' }))
  fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'target' } })
  fireEvent.change(screen.getByLabelText('Endpoint'), { target: { value: 'target:9000' } })
  fireEvent.change(screen.getByLabelText('Access key'), { target: { value: 'AK' } })
  fireEvent.change(screen.getByPlaceholderText('Secret key'), { target: { value: 'secret' } })
  expect(screen.getByRole('button', { name: 'Save cluster' })).toBeDisabled()
  fireEvent.click(screen.getByRole('button', { name: 'Run probe' }))
  expect(await screen.findByText('Credential probe measured: reachable')).toBeInTheDocument()
  expect(screen.getByText(/Capability defaults are assumed/)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Save cluster' }))
  expect(await screen.findByText('Cluster target added')).toBeInTheDocument()
  expect(fetchMock.mock.calls.some(([url, init]) => String(url).endsWith('/v1/clusters') && (init as RequestInit)?.method === 'POST')).toBe(true)
})

test('adopts a bucket, expands it under a chosen name, and hands off to Migrations', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [] }
  let expandBody: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/placements/acme/data/adopt')) {
      const placement: PlacementStatus = { key: 'acme/data', state: 'ACTIVE', primary: 'source', names: { source: 'data' }, read_only: false, reject_writes: false }
      directory.placements = [placement]; directory.version++
      return Response.json(placement)
    }
    if (url.endsWith('/placements/acme/data/view')) {
      const placement = directory.placements[0]
      return Response.json({ ...placement, fence: { version: directory.version, held: false, proxies: 0, waiting_on: [], silent: [] }, operations: [], clusters: { source, target } })
    }
    if (url.endsWith('/placements/acme/data/expand') && init?.method === 'POST') {
      expandBody = JSON.parse(String(init.body))
      directory.placements[0] = { ...directory.placements[0], target: 'target', names: { source: 'data', target: 'data-archive' } }
      directory.version++
      return Response.json({ key: 'acme/data', target: 'target', name: 'data-001', version: directory.version })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Adopt or create' }))
  fireEvent.change(screen.getByLabelText('Tenant'), { target: { value: 'acme' } })
  fireEvent.change(screen.getByLabelText('Client bucket'), { target: { value: 'data' } })
  fireEvent.click(screen.getByRole('button', { name: 'Adopt bucket' }))
  expect(await screen.findByText('Bucket acme/data adopted')).toBeInTheDocument()
  fireEvent.click(await screen.findByRole('button', { name: 'Expand acme/data' }))
  expect(screen.getByRole('dialog')).toHaveTextContent('Expand acme/data')
  expect(screen.getByText('data-001')).toBeInTheDocument()
  expect(screen.getByLabelText('Target bucket name')).toHaveAttribute('placeholder', 'data-001')
  fireEvent.change(screen.getByLabelText('Target bucket name'), { target: { value: ' data-archive ' } })
  fireEvent.click(screen.getByRole('button', { name: 'Create target and continue to Migrations' }))
  await waitFor(() => expect(expandBody).toEqual({ to: 'target', name: 'data-archive', create: true }))
  await waitFor(() => expect(screen.getByRole('heading', { name: 'Migrations', level: 1 })).toBeInTheDocument())
  expect(await screen.findByRole('heading', { name: 'source → target' })).toBeInTheDocument()
})

test('create imports a client key, and a bucket no key reaches is flagged', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [source], placements: [] }
  let sent: unknown
  let imported: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/placements/acme/fresh/create-backend') && init?.method === 'POST') {
      sent = JSON.parse(String(init.body))
      const placement: PlacementStatus = { key: 'acme/fresh', state: 'ACTIVE', primary: 'source', names: { source: 'fresh' }, read_only: false, reject_writes: false, client_keys: 1 }
      directory.placements = [placement, { ...placement, key: 'acme/bare', names: { source: 'bare' }, client_keys: 0 }]
      directory.version++
      return Response.json(placement)
    }
    if (url.endsWith('/v1/tenants/acme/client-keys') && init?.method === 'POST') {
      imported = JSON.parse(String(init.body))
      directory.placements[1] = { ...directory.placements[1], client_keys: 1 }
      directory.version++
      return Response.json({ access_key: 'LATEKEY', tenant: 'acme', checked: 'source' })
    }
    if (url.endsWith('/placements/acme/bare/view')) {
      return Response.json({ ...directory.placements[1], fence: { version: directory.version, held: false, proxies: 0, waiting_on: [], silent: [] }, operations: [], clusters: { source } })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Adopt or create' }))
  fireEvent.click(screen.getByRole('button', { name: 'Create new' }))
  expect(screen.getByText(/only with a client key shunt holds/)).toBeInTheDocument()
  fireEvent.change(screen.getByLabelText('Tenant'), { target: { value: 'acme' } })
  fireEvent.change(screen.getByLabelText('Client bucket'), { target: { value: 'fresh' } })
  fireEvent.change(screen.getByLabelText('Access key'), { target: { value: ' CLIENTKEY ' } })
  fireEvent.change(screen.getByLabelText('Secret'), { target: { value: 'client-secret' } })
  fireEvent.click(screen.getByRole('button', { name: 'Create bucket' }))
  expect(await screen.findByText('Bucket acme/fresh created')).toBeInTheDocument()
  expect(sent).toEqual({ cluster: 'source', name: 'fresh', keys: [{ access_key: 'CLIENTKEY', secret: 'client-secret' }] })

  expect(await screen.findByText('no client keys')).toBeInTheDocument()
  fireEvent.click(screen.getByText('no client keys'))
  expect(await screen.findByRole('alert')).toHaveTextContent('No client key can reach this bucket')
  expect(screen.getByText(/shunt client add <access-key> --tenant acme --check source/)).toBeInTheDocument()

  // The same import from the browser: checked against the primary, limited to the bucket on request.
  fireEvent.change(screen.getByLabelText('Client access key'), { target: { value: 'LATEKEY' } })
  fireEvent.change(screen.getByLabelText('Client secret'), { target: { value: 'late-secret' } })
  fireEvent.click(screen.getByLabelText(/Limit this key to bare/))
  fireEvent.click(screen.getByRole('button', { name: 'Import client key' }))
  expect(await screen.findByText('Client key LATEKEY imported for acme/bare')).toBeInTheDocument()
  expect(imported).toEqual({ access_key: 'LATEKEY', secret: 'late-secret', cluster: 'source', buckets: ['bare'] })
  await waitFor(() => expect(screen.queryByText('no client keys')).not.toBeInTheDocument())
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
})

test('naming an existing client bucket offers Expand instead of adding it twice', async () => {
  const placement: PlacementStatus = { key: 'acme/data', state: 'ACTIVE', primary: 'source', names: { source: 'data' }, read_only: false, reject_writes: false }
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [placement] }
  let expandBody: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/placements/acme/data/expand') && init?.method === 'POST') {
      expandBody = JSON.parse(String(init.body))
      return Response.json({ key: 'acme/data', target: 'target', name: 'data-minio', version: 4 })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Adopt or create' }))
  fireEvent.click(screen.getByRole('button', { name: 'Create new' }))
  fireEvent.change(screen.getByLabelText('Tenant'), { target: { value: 'acme' } })
  expect(document.querySelector('#client-buckets option[value="data"]')).not.toBeNull()
  fireEvent.change(screen.getByLabelText('Client bucket'), { target: { value: 'data' } })
  fireEvent.change(screen.getByLabelText('Cluster'), { target: { value: 'target' } })
  fireEvent.change(screen.getByLabelText(/Backend bucket name/), { target: { value: 'data-minio' } })
  expect(screen.getByRole('status')).toHaveTextContent('acme/data already exists: ACTIVE on source as data')
  expect(screen.getByRole('button', { name: 'Create bucket' })).toBeDisabled()

  fireEvent.click(screen.getByRole('button', { name: 'Expand data to target' }))
  expect(screen.getByRole('dialog')).toHaveTextContent('Expand acme/data')
  expect(screen.getByLabelText('Target bucket name')).toHaveValue('data-minio')
  fireEvent.click(screen.getByRole('button', { name: 'Create target and continue to Migrations' }))
  await waitFor(() => expect(expandBody).toEqual({ to: 'target', name: 'data-minio', create: true }))
})

test('clears an expanded target from the row, and expand can accept existing objects', async () => {
  const placement: PlacementStatus = { key: 'acme/data', state: 'ACTIVE', primary: 'source', target: 'target', names: { source: 'data', target: 'old' }, read_only: false, reject_writes: false }
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [placement] }
  let cleared = false
  let expandBody: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/placements/acme/data/target') && init?.method === 'DELETE') {
      cleared = true
      directory.placements = [{ ...placement, target: undefined, names: { source: 'data' } }]
      directory.version++
      return Response.json({ key: 'acme/data', target: 'target', name: 'old', version: directory.version })
    }
    if (url.endsWith('/placements/acme/data/expand') && init?.method === 'POST') {
      expandBody = JSON.parse(String(init.body))
      return Response.json({ key: 'acme/data', target: 'target', name: 'old', version: 5 })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  expect(screen.queryByRole('button', { name: 'Expand acme/data' })).not.toBeInTheDocument()
  fireEvent.click(await screen.findByRole('button', { name: 'Clear target of acme/data' }))
  expect(await screen.findByText('acme/data: target target cleared; its bucket is still there')).toBeInTheDocument()
  expect(cleared).toBe(true)

  fireEvent.click(await screen.findByRole('button', { name: 'Expand acme/data' }))
  fireEvent.change(screen.getByLabelText('Target bucket name'), { target: { value: 'old' } })
  fireEvent.click(screen.getByLabelText(/already holds this bucket's objects/))
  fireEvent.click(screen.getByRole('button', { name: 'Create target and continue to Migrations' }))
  await waitFor(() => expect(expandBody).toEqual({ to: 'target', name: 'old', create: true, accept_existing_objects: true }))
})

test('creates a bucket spread across clusters and shows its legs', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [] }
  let sent: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/placements/acme/wide/create-backend') && init?.method === 'POST') {
      sent = JSON.parse(String(init.body))
      const placement: PlacementStatus = { key: 'acme/wide', state: 'ACTIVE', primary: '', names: {}, read_only: false, reject_writes: false,
        legs: [{ id: 'source', cluster: 'source', bucket: 'wide', share: 0.5, ranges: [{ from: '0000000000000000', to: '7fffffffffffffff' }] }, { id: 'target', cluster: 'target', bucket: 'wide', share: 0.5, ranges: [{ from: '8000000000000000', to: 'ffffffffffffffff' }] }] }
      directory.placements = [placement]
      directory.version++
      return Response.json(placement)
    }
    if (url.endsWith('/placements/acme/wide/view')) return Response.json({ ...directory.placements[0], fence: { version: directory.version, held: false, proxies: 0, waiting_on: [], silent: [] }, operations: [], clusters: { source, target } })
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Adopt or create' }))
  fireEvent.click(screen.getByRole('button', { name: 'Create new' }))
  fireEvent.change(screen.getByLabelText('Tenant'), { target: { value: 'acme' } })
  fireEvent.change(screen.getByLabelText('Client bucket'), { target: { value: 'wide' } })
  fireEvent.click(screen.getByLabelText('source'))
  fireEvent.click(screen.getByLabelText('target'))
  expect(screen.getByText('(not used: spreading)')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Create bucket' }))
  expect(await screen.findByText('Bucket acme/wide created, spread over 2 clusters')).toBeInTheDocument()
  expect(sent).toEqual({ cluster: 'source', name: 'wide', legs: [{ cluster: 'source', name: 'wide' }, { cluster: 'target', name: 'wide' }] })
  expect(await screen.findByText('spread: source + target')).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Expand acme/wide' })).not.toBeInTheDocument()
  fireEvent.click(screen.getByText('spread: source + target'))
  expect(await screen.findAllByText('wide · 50% of keys', { exact: false })).toHaveLength(2)
})

test('moves part of a spread bucket leg to another cluster', async () => {
  const placement: PlacementStatus = { key: 'acme/wide', state: 'ACTIVE', primary: '', names: {}, read_only: false, reject_writes: false,
    legs: [{ id: 'source', cluster: 'source', bucket: 'wide', share: 0.5, ranges: [{ from: '0000000000000000', to: '7fffffffffffffff' }] },
      { id: 'target', cluster: 'target', bucket: 'wide', share: 0.5, ranges: [{ from: '8000000000000000', to: 'ffffffffffffffff' }] }] }
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [placement] }
  let started: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/v1/operations') && init?.method === 'POST') {
      started = JSON.parse(String(init.body))
      return Response.json({ id: 'op-1', kind: 'ramp', placement: 'acme/wide', actor: 'token:x', status: 'running', phase: 'queued' })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Move keys of acme/wide' }))
  fireEvent.change(screen.getByLabelText("Share of the leg's keys"), { target: { value: '0.5' } })
  expect(screen.getByText('Into its leg\'s bucket', { exact: false })).toHaveTextContent('wide')
  fireEvent.click(screen.getByRole('button', { name: 'Start the move and continue to Migrations' }))
  await waitFor(() => expect(started).toEqual({ kind: 'ramp', placement: 'acme/wide',
    args: { to: 'target', range: { from: '0000000000000000', to: '3fffffffffffffff' }, ratio: 0.01, create: true, wait: '30s' } }))
})

test('moves a bucket to another bucket on its own cluster from Expand', async () => {
  const placement: PlacementStatus = { key: 'acme/data', state: 'ACTIVE', primary: 'source', names: { source: 'data' }, read_only: false, reject_writes: false }
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [placement] }
  let started: unknown
  baseMock(directory, (url, init) => {
    if (url.endsWith('/v1/operations') && init?.method === 'POST') {
      started = JSON.parse(String(init.body))
      return Response.json({ id: 'op-1', kind: 'ramp', placement: 'acme/data', actor: 'token:x', status: 'running', phase: 'queued' })
    }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Buckets' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Expand acme/data' }))
  fireEvent.change(screen.getByLabelText('Target cluster'), { target: { value: 'source' } })
  expect(screen.getByText(/cannot be recorded as a target/)).toBeInTheDocument()
  fireEvent.change(screen.getByLabelText('Target bucket name'), { target: { value: 'data-final' } })
  fireEvent.click(screen.getByRole('button', { name: 'Start the move and continue to Migrations' }))
  await waitFor(() => expect(started).toEqual({ kind: 'ramp', placement: 'acme/data',
    args: { to: 'source', name: 'data-final', ratio: 0.01, create: true, wait: '30s' } }))
})
