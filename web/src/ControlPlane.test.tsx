import { fireEvent, render, screen, within } from '@testing-library/react'
import { App } from './App'
import { StoreProvider } from './store'

const control = {
  node: 'c1', version: 'test', directory: 12, directory_loaded: true, last_compaction: null, join: 'shunt-control join --name <name>', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: ['http://peer-1'], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 42, db_bytes: 1, db_in_use_bytes: 1, quota_bytes: 2, leader: 'c1' },
}
const unclean = { id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', state: 'unclean', uncertain: 2, ended: '2026-09-24T12:00:00Z' }
const members = [
  { id: 'proxy-a', live: true, applied: 12, durable: 12, seq: 3, host: 'host-a', incarnation: { id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', state: 'active', started: '2026-09-24T11:00:00Z' } },
  { id: 'proxy-b', live: false, applied: 12, durable: 12, seq: 4, host: 'host-b', unresolved: [unclean] },
  { id: 'proxy-c', live: false, applied: 12, durable: 12, seq: 5, host: 'host-c', incarnation: { id: 'cccccccccccccccccccccccccccccccc', state: 'retired', ended: '2026-09-24T12:30:00Z' } },
]

beforeEach(() => {
  sessionStorage.setItem('shunt.control.token', 'secret')
})
afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

function mock(onCall: (url: string, init?: RequestInit) => Response | undefined) {
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    const answered = onCall(url, init)
    if (answered) return answered
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 12, members })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 12, clusters: [], placements: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    for (const m of members) if (url.endsWith(`/v1/fleet/${m.id}`) && (init?.method ?? 'GET') === 'GET') return Response.json({ ...m, directory: 12, lineage: true, behind: 0, backpressure: false, secrets: [], problems: [] })
    return Response.json({ message: `unhandled ${init?.method ?? 'GET'} ${url}` }, { status: 404 })
  })
}

test('shows each proxy\'s incarnation state, resolves an unclean one with an attestation, and shows a refused forget', async () => {
  let resolved: unknown
  mock((url, init) => {
    if (url.endsWith('/v1/fleet/proxy-b/resolve') && init?.method === 'POST') { resolved = JSON.parse(String(init.body)); return Response.json({ member: { ...members[1], unresolved: [] } }) }
    if (url.endsWith('/v1/fleet/proxy-b') && init?.method === 'DELETE') return Response.json({ code: 'retirement_unproven', message: 'forgetting proxy proxy-b refused: proxy proxy-b has 1 incarnation(s) that did not retire cleanly', retryable: false }, { status: 409 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  const table = await screen.findByText('Proxy fleet')
  expect(table).toBeInTheDocument()
  const rows = screen.getAllByRole('row')
  expect(rows.some((row) => within(row).queryByText('proxy-a') && within(row).queryByText('live'))).toBe(true)
  expect(rows.some((row) => within(row).queryByText('proxy-b') && within(row).queryByText('unresolved (1)'))).toBe(true)
  expect(rows.some((row) => within(row).queryByText('proxy-c') && within(row).queryByText('retired'))).toBe(true)

  fireEvent.click(screen.getByText('proxy-b'))
  expect(await screen.findByText(/did not retire cleanly \(2 outcomes unknown/)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Forget proxy' }))
  expect(await screen.findByText(/forgetting proxy proxy-b refused/)).toBeInTheDocument()
  const resolve = screen.getByRole('button', { name: 'Resolve incarnation' })
  expect(resolve).toBeDisabled()
  fireEvent.change(screen.getByLabelText(`Attestation for ${unclean.id}`), { target: { value: 'host wiped on 2026-09-24' } })
  fireEvent.click(resolve)
  expect(await screen.findByText(/Incarnation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa of proxy-b resolved/)).toBeInTheDocument()
  expect(resolved).toEqual({ incarnation: unclean.id, attestation: 'host wiped on 2026-09-24' })
})

test('asks a live proxy to retire', async () => {
  let retired = ''
  mock((url, init) => {
    if (url.endsWith('/v1/fleet/proxy-a/retire') && init?.method === 'POST') { retired = url; return Response.json({ member: { ...members[0], retire_requested: true }, retire_requested: true }) }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Proxy fleet')
  fireEvent.click(screen.getByText('proxy-a'))
  fireEvent.click(await screen.findByRole('button', { name: 'Retire proxy' }))
  expect(await screen.findByText(/retirement requested; it drains and stops at its next heartbeat/)).toBeInTheDocument()
  expect(retired).toMatch(/\/v1\/fleet\/proxy-a\/retire$/)
})
