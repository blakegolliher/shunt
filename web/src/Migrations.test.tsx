import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { App } from './App'
import type { ClusterStatus, DirectoryStatus, PlacementView } from './api/client'
import { StoreProvider } from './store'

const source: ClusterStatus = { name: 'source', type: 's3', scheme: 'http', region: 'garage', endpoints: ['source:9000'], access_key: 'AK', secret_ref: 'control:source', conditional_write: false, conditional_delete: false, references: ['placements.default/data'], read_only: false, reject_writes: false }
const target: ClusterStatus = { ...source, name: 'target', region: 'us-east-1', endpoints: ['target:9000'], secret_ref: 'control:target', conditional_write: true, references: ['placements.default/data'] }
const control = {
  node: 'c1', version: 'test', directory: 4, directory_loaded: true, last_compaction: null,
  join: 'shunt-control join --name <name>', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: ['http://c1:2380'], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 4, db_bytes: 1024, db_in_use_bytes: 512, quota_bytes: 2048, leader: 'c1' },
}

function expandedView(): PlacementView {
  return {
    key: 'default/data', state: 'ACTIVE', primary: 'source', target: 'target', names: { source: 'data', target: 'data-001' },
    read_only: false, reject_writes: false, ratio: 0, prefixes: [], ramp_writes: {}, fallback_reads: 0, dual_deletes: {},
    fence: { version: 4, held: false, proxies: 2, waiting_on: [], silent: [] }, operations: [], clusters: { source, target },
    source_uploads_in_flight: null,
  }
}

function migrationMock(view: PlacementView, mutate?: (url: string, init?: RequestInit) => Response | undefined) {
  const directory: DirectoryStatus = { version: view.fence.version, clusters: [source, target], placements: [view] }
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    const changed = mutate?.(url, init)
    if (changed) return changed
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: directory.version, members: [{ id: 'proxy-a', live: true, applied: directory.version, seq: 2 }, { id: 'proxy-b', live: true, applied: directory.version, seq: 2 }] })
    if (url.endsWith('/v1/status?all=1')) return Response.json(directory)
    if (url.endsWith('/placements/default/data/view')) return Response.json(view)
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${init?.method ?? 'GET'} ${url}` }, { status: 404 })
  })
}

beforeEach(() => sessionStorage.setItem('shunt.control.token', 'actor-token'))
afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

async function openMigrations() {
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Migrations' }))
  await screen.findByRole('heading', { name: 'source → target' })
}

test('applies the 50 percent ramp through an operation and keeps fence state visible', async () => {
  const view = expandedView()
  const fetchMock = migrationMock(view, (url, init) => {
    if (url.endsWith('/v1/operations') && init?.method === 'POST') return Response.json({ id: 'op-ramp', kind: 'ramp', placement: view.key, actor: 'token:123', status: 'running', phase: 'hold', waiting_on: ['proxy-b'] }, { status: 202 })
    if (url.endsWith('/v1/operations/op-ramp')) return Response.json({ id: 'op-ramp', kind: 'ramp', placement: view.key, actor: 'token:123', status: 'succeeded', phase: 'done', version: 6 })
  })
  await openMigrations()
  expect(screen.getByText(/Applied at directory revision/)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: '50%' }))
  fireEvent.click(screen.getByRole('button', { name: 'Apply ramp' }))
  expect(await screen.findByText('ramp started')).toBeInTheDocument()
  expect(await screen.findByText(/hold: waiting on proxy-b/)).toBeInTheDocument()
  const start = fetchMock.mock.calls.find(([url, init]) => String(url).endsWith('/v1/operations') && (init as RequestInit)?.method === 'POST')
  expect(JSON.parse(String((start?.[1] as RequestInit).body))).toEqual({ kind: 'ramp', placement: 'default/data', args: { ratio: 0.5, prefixes: [], wait: '30s' } })
  await waitFor(() => expect(screen.getByText('ramp succeeded')).toBeInTheDocument(), { timeout: 1500 })
})

test('shows a stale proxy warning and the mover lost-write acceptance text', async () => {
  const view = { ...expandedView(), state: 'MIGRATING', primary: 'target', source: 'source', target: undefined, ratio: 1, clusters: { source, target: { ...target, conditional_write: false } }, mover: undefined }
  migrationMock(view, (url) => {
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 8, members: [{ id: 'proxy-a', live: true, applied: 8, seq: 3 }, { id: 'proxy-b', live: false, applied: 7, seq: 2 }] })
  })
  await openMigrations()
  expect(screen.getByRole('alert')).toHaveTextContent('Stale proxy-b')
  expect(screen.getByText(/a client write between the mover's HEAD and PUT can be overwritten/)).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Start mover' })).toBeDisabled()
  fireEvent.click(screen.getByLabelText(/I accept the lost-write window/))
  expect(screen.getByRole('button', { name: 'Start mover' })).toBeEnabled()
})

test('reaches purge before cutover and renders the API refusal verbatim', async () => {
  const view = { ...expandedView(), state: 'RAMPING', primary: 'target', source: 'source', target: undefined, ratio: 0.5 }
  const reason = 'default/data is RAMPING; purge-source runs on a placement in CUTOVER'
  migrationMock(view, (url, init) => {
    if (url.endsWith('/placements/default/data/purge-source') && init?.method === 'POST') return Response.json({ allowed: false, reason, key: view.key, objects: 0, bytes: 0, uploads_in_flight: 0, missing: [], version: 8 })
  })
  await openMigrations()
  fireEvent.click(screen.getByRole('button', { name: 'Dry-run purge' }))
  expect((await screen.findAllByText(reason)).length).toBeGreaterThanOrEqual(1)
  expect(screen.getByRole('button', { name: 'Confirm' })).toBeDisabled()
})
