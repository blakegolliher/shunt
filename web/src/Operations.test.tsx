import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
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

// An attestation is evidence about one record's worker. It must not carry over to another record,
// is cleared once the resolution is recorded, and survives a failed request so it can be corrected.
describe('worker attestation', () => {
  const sessionA = '00112233445566778899aabbccddeeff'
  const sessionB = 'ffeeddccbbaa99887766554433221100'
  const mover = (id: string, session: string): Operation => ({ id, kind: 'mover', placement: 'acme/data', actor: 'api:10.0.0.1', node: 'c1', status: 'blocked', effect_state: 'none',
    scope: { resource: 'placement:acme/data', generation: 9 }, phase: 'mover', updated: '2026-09-24T12:00:03Z', allowed_actions: ['resolve-worker'],
    blockers: [{ code: 'worker_unresolved', message: `worker session ${session} expired with its work unresolved` }],
    worker: { id: session, state: 'active', sequence: 4, inflight: 1, uncertain: 1 } })
  const a = mover('1790000000005-aaaaaa', sessionA)
  const b = mover('1790000000004-bbbbbb', sessionB)

  // serve answers the screen's reads, and resolve-worker with answer.
  function serve(answer: (op: Operation, body: { session: string; attestation: string }) => Response) {
    const posted: { url: string; body: { session: string; attestation: string } }[] = []
    vi.mocked(fetch).mockImplementation(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/control')) return Response.json(control)
      if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
      if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 3, clusters: [], placements: [] })
      if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
      if (url.includes('/v1/operations?limit=')) return Response.json({ operations: [a, b] })
      const op = [a, b].find((o) => url.endsWith(`/v1/operations/${o.id}/resolve-worker`))
      if (op && init?.method === 'POST') {
        const body = JSON.parse(String(init.body)) as { session: string; attestation: string }
        posted.push({ url, body })
        return answer(op, body)
      }
      return Response.json({ message: `unhandled ${url}` }, { status: 404 })
    })
    return posted
  }
  const box = () => screen.getByLabelText(/known to have ended, say how/) as HTMLTextAreaElement
  async function open() {
    render(<StoreProvider><App /></StoreProvider>)
    fireEvent.click(await screen.findByRole('button', { name: 'Operations' }))
    fireEvent.click(await screen.findByRole('button', { name: `Show operation ${a.id}` }))
  }

  test('is cleared when another operation is selected', async () => {
    const posted = serve((op) => Response.json(op))
    await open()
    fireEvent.change(box(), { target: { value: 'host of A is off' } })
    expect(box().value).toBe('host of A is off')
    fireEvent.click(screen.getByRole('button', { name: `Show operation ${b.id}` }))
    expect(box().value).toBe('')
    expect(screen.getByRole('button', { name: 'Resolve worker session' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: `Show operation ${a.id}` }))
    expect(box().value).toBe('') // never restored onto the record it was typed for, either
    expect(posted).toEqual([])
  })

  test('sends the selected record\'s session and the trimmed attestation, and is cleared once recorded', async () => {
    // The answer still offers resolve-worker: the owner ends the record at its next poll.
    const posted = serve((op, body) => Response.json({ ...op, worker: { ...op.worker, state: 'completed', resolved_by: 'api:10.0.0.1', attestation: body.attestation } }))
    await open()
    fireEvent.click(screen.getByRole('button', { name: `Show operation ${b.id}` }))
    fireEvent.change(box(), { target: { value: '  host of B is off\n' } })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve worker session' }))
    expect(await screen.findByText(new RegExp(`worker session ${sessionB} resolved`))).toBeInTheDocument()
    expect(posted).toEqual([{ url: `/v1/operations/${b.id}/resolve-worker`, body: { session: sessionB, attestation: 'host of B is off' } }])
    await waitFor(() => expect(box().value).toBe(''))
  })

  test('is kept when the request fails, so it can be corrected and sent again', async () => {
    let fail = true
    const posted = serve((op, body) => fail
      ? Response.json({ code: 'refused', message: `worker session ${sessionA} is live (last heartbeat 200ms ago)` }, { status: 409 })
      : Response.json({ ...op, worker: { ...op.worker, state: 'completed', resolved_by: 'api:10.0.0.1', attestation: body.attestation } }))
    await open()
    fireEvent.change(box(), { target: { value: 'host of A is off' } })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve worker session' }))
    expect(await screen.findByText(/is live \(last heartbeat/)).toBeInTheDocument()
    await waitFor(() => expect(screen.getByRole('button', { name: 'Resolve worker session' })).not.toBeDisabled())
    expect(box().value).toBe('host of A is off')

    fail = false
    fireEvent.click(screen.getByRole('button', { name: 'Resolve worker session' }))
    expect(await screen.findByText(new RegExp(`worker session ${sessionA} resolved`))).toBeInTheDocument()
    await waitFor(() => expect(box().value).toBe(''))
    expect(posted.map((p) => p.body)).toEqual([{ session: sessionA, attestation: 'host of A is off' }, { session: sessionA, attestation: 'host of A is off' }])
  })
})
