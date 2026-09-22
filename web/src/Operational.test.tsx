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

test('adopts a bucket, shows the generated expand name, and hands off to Migrations', async () => {
  const directory: DirectoryStatus = { version: 3, clusters: [source, target], placements: [] }
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
      directory.placements[0] = { ...directory.placements[0], target: 'target', names: { source: 'data', target: 'data-001' } }
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
  fireEvent.click((await screen.findAllByText('data'))[0])
  fireEvent.click(await screen.findByRole('button', { name: 'Expand' }))
  expect(screen.getByText('data-001')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Create target and continue to Migrations' }))
  await waitFor(() => expect(screen.getByRole('heading', { name: 'Migrations', level: 1 })).toBeInTheDocument())
  expect(screen.getByText(/acme\/data is ready/)).toBeInTheDocument()
})
