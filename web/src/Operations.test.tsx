import { fireEvent, render, screen, within } from '@testing-library/react'
import { App } from './App'
import type { Operation } from './api/client'
import { StoreProvider } from './store'

const control = {
  node: 'c1', version: 'test', directory: 3, directory_loaded: true, last_compaction: null, join: '', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: ['http://c1:2380'], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 3, db_bytes: 1, db_in_use_bytes: 1, quota_bytes: 2, leader: 'c1' },
}

const ops: Operation[] = [
  { id: '1790000000002-aaaaaa', kind: 'ramp', placement: 'acme/data', actor: 'api:10.0.0.1', node: 'c2', status: 'failed', effect_state: 'uncertain',
    scope: { resource: 'placement:acme/data', generation: 41, clusters: ['vast01', 'vast02'] }, phase: 'done', updated: '2026-09-24T12:00:02Z',
    error: { code: 'unavailable', message: 'control node c2 stopped running this operation' }, allowed_actions: [], request_id: 'cli-7' },
  { id: '1790000000001-bbbbbb', kind: 'cluster-read-only', cluster: 'vast03', actor: 'api:10.0.0.1', status: 'running', effect_state: 'none',
    scope: { resource: 'cluster:vast03', generation: 7 }, phase: 'hold', waiting_on: ['proxy-b'], updated: '2026-09-24T12:00:01Z', allowed_actions: [] },
]

beforeEach(() => {
  sessionStorage.setItem('shunt.control.token', 'actor-token')
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 3, clusters: [], placements: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    if (url.includes('/v1/operations?limit=')) return Response.json({ operations: ops })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
})
afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

test('lists operation records with their scope, status and effect, and explains an uncertain one', async () => {
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Operations' }))
  const table = await screen.findByRole('table')
  expect(within(table).getByText('placement:acme/data')).toBeInTheDocument()
  expect(within(table).getByText('cluster:vast03')).toBeInTheDocument()
  expect(within(table).getByText('uncertain')).toBeInTheDocument()
  expect(within(table).getByText('hold')).toBeInTheDocument() // an unfinished record shows its phase

  fireEvent.click(screen.getByRole('button', { name: 'Show operation 1790000000002-aaaaaa' }))
  expect(screen.getByText(/repeat the step to complete it/)).toBeInTheDocument()
  expect(screen.getByText('vast01, vast02')).toBeInTheDocument()
  expect(screen.getByRole('alert')).toHaveTextContent('unavailable: control node c2 stopped running this operation')
  expect(screen.getByText('cli-7')).toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: 'Show operation 1790000000001-bbbbbb' }))
  expect(screen.getByText('proxy-b')).toBeInTheDocument()
})

// Defect 7 of the H2 review: an external mover whose worker session expired is resolved from the
// Operations screen, with the session its record names and the operator's attestation.
test('resolves an expired external mover worker session with an attestation', async () => {
  const session = '00112233445566778899aabbccddeeff'
  const mover: Operation = { id: '1790000000003-cccccc', kind: 'mover', placement: 'acme/data', actor: 'api:10.0.0.1', node: 'c1', status: 'blocked', effect_state: 'none',
    scope: { resource: 'placement:acme/data', generation: 9 }, phase: 'mover', updated: '2026-09-24T12:00:03Z', allowed_actions: ['resolve-worker'],
    blockers: [{ code: 'worker_unresolved', message: `worker session ${session} expired with its work unresolved` }],
    worker: { id: session, state: 'active', sequence: 4, inflight: 1, uncertain: 1 } }
  const posted: { url: string; body: unknown }[] = []
  vi.mocked(fetch).mockImplementation(async (input, init) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 3, clusters: [], placements: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    if (url.includes('/v1/operations?limit=')) return Response.json({ operations: [mover] })
    if (url.endsWith(`/v1/operations/${mover.id}/resolve-worker`) && init?.method === 'POST') {
      posted.push({ url, body: JSON.parse(String(init.body)) })
      return Response.json({ ...mover, status: 'blocked', allowed_actions: [], worker: { ...mover.worker, state: 'completed', resolved_by: 'api:10.0.0.1', attestation: 'host off' } })
    }
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Operations' }))
  fireEvent.click(await screen.findByRole('button', { name: `Show operation ${mover.id}` }))
  expect(screen.getByText(session)).toBeInTheDocument()
  const resolve = screen.getByRole('button', { name: 'Resolve worker session' })
  expect(resolve).toBeDisabled() // no attestation yet
  fireEvent.change(screen.getByLabelText(/known to have ended, say how/), { target: { value: 'host off' } })
  fireEvent.click(resolve)
  expect(await screen.findByText(new RegExp(`worker session ${session} resolved`))).toBeInTheDocument()
  expect(posted).toEqual([{ url: `/v1/operations/${mover.id}/resolve-worker`, body: { session, attestation: 'host off' } }])
})
