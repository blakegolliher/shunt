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

test('shows each control member\'s observed health, never health from a name, and quorum as observed', async () => {
  const observed = {
    ...control,
    cluster: { ...control.cluster, members: [...control.cluster.members, { name: 'c2', id: '2', peer_urls: ['http://peer-2'], leader: false, started: true }, { name: '', id: 'beef', peer_urls: ['http://peer-9'], leader: false, learner: true, started: false }], quorum: 2, has_quorum: false },
    partial: true,
    members: [
      { id: '1', name: 'c1', role: 'voter', leader: true, peer_urls: ['http://peer-1'], started: true, health: 'healthy', observed_at: '2026-09-28T12:00:00Z', age_ms: 2000, observer: 'c1' },
      { id: '2', name: 'c2', role: 'voter', peer_urls: ['http://peer-2'], started: true, health: 'unreachable', observed_at: '2026-09-28T12:00:00Z', age_ms: 2000, observer: 'c1', reason: 'connection refused: shunt-control is not running there' },
      { id: 'beef', role: 'learner', peer_urls: ['http://peer-9'], started: false, health: 'unknown', observed_at: '2026-09-28T12:00:00Z', age_ms: 2000, observer: 'c1', reason: 'added and never started: nothing to probe' },
    ],
    quorum: { state: 'unavailable', observed_at: '2026-09-28T12:00:00Z', age_ms: 1000, observer: 'c1', method: 'linearizable_read', error_code: 'timeout' },
  }
  mock((url) => (url.endsWith('/v1/control') ? Response.json(observed) : undefined))
  render(<StoreProvider><App /></StoreProvider>)
  expect(await screen.findByText('Control members')).toBeInTheDocument()
  expect(screen.getByText('Unavailable')).toBeInTheDocument()
  expect(screen.getByText(/timeout, read 1s ago by c1/)).toBeInTheDocument()
  expect(screen.getByText(/Partial: quorum is not reachable/)).toBeInTheDocument()
  expect(screen.getByText(/connection refused: shunt-control is not running there/)).toBeInTheDocument()
  expect(screen.getByText('(unnamed)')).toBeInTheDocument()
  expect(screen.getByText(/added and never started/)).toBeInTheDocument()
  // c2 has a name and once started; it is not healthy for that.
  expect(screen.getAllByText(/^healthy/)).toHaveLength(1)
  expect(screen.getByText(/^unreachable/)).toBeInTheDocument()
  expect(screen.getByText(/^unknown/)).toBeInTheDocument()
})

const joinTemplate = 'shunt-control join --name <name> --data-dir <data-dir> --peer-url http://<host>:2380 --api <host>:9901 --existing http://c1:9901'
const threeMembers = {
  ...control,
  join: joinTemplate,
  cluster: { ...control.cluster, quorum: 2 },
  quorum: { state: 'reachable', observed_at: '2026-09-28T12:00:00Z', age_ms: 1000, observer: 'c1', method: 'linearizable_read' },
  members: [
    { id: '1', name: 'c1', role: 'voter', leader: true, peer_urls: ['http://peer-1'], started: true, health: 'healthy', observed_at: '2026-09-28T12:00:00Z', age_ms: 1000 },
    { id: '2', name: 'c2', role: 'voter', peer_urls: ['http://peer-2'], started: true, health: 'healthy', observed_at: '2026-09-28T12:00:00Z', age_ms: 1000 },
    { id: 'beef', role: 'learner', peer_urls: ['http://peer-9'], started: false, health: 'unknown', reason: 'added and never started: nothing to probe' },
  ],
}
const joinOp = (status: string, extra: Record<string, unknown> = {}) => ({
  id: 'join-1', kind: 'control-join', actor: 'token:abc', status, phase: 'learner_added', scope: { resource: 'control:members', generation: 1 },
  member: { id: '8e9e05c52164694d', peer_url: 'http://10.0.0.4:2380' }, args: { name: 'c4', peer_url: 'http://10.0.0.4:2380' },
  blockers: [{ code: 'member_not_started', message: 'waiting for c4 to fetch its bootstrap and start (`shunt-control join` on that host)' }], allowed_actions: ['cancel'], ...extra,
})

// T12 in the browser (H3e): the join wizard records the join's intent, keeping its Idempotency-Key
// across a retry, and then shows the command the new host runs with --resume; the browser never asks
// for the bootstrap, which carries the encryption key. It follows the learner's progress, and a
// cancel is announced only when the record ends cancelled.
test('joins a control member from the wizard: intent, the node\'s command, progress and cancel', async () => {
  const posts: { body: unknown; key: string | null }[] = []
  let cancelled = false
  const urls: string[] = []
  mock((url, init) => {
    urls.push(url)
    if (url.endsWith('/v1/control')) return Response.json(threeMembers)
    if (url.endsWith('/v1/control/members') && init?.method === 'POST') {
      posts.push({ body: JSON.parse(String(init.body)), key: new Headers(init.headers).get('Idempotency-Key') })
      if (posts.length === 1) return Response.json({ code: 'unavailable', message: 'the answer was lost on the way' }, { status: 503 })
      return Response.json(joinOp('blocked'), { status: 202 })
    }
    if (url.endsWith('/v1/operations/join-1/cancel')) { cancelled = true; return Response.json(joinOp('blocked', { cancel_request: { actor: 'token:abc', at: '2026-09-28T12:00:01Z' }, allowed_actions: [] }), { status: 202 }) }
    if (url.endsWith('/v1/operations/join-1')) return Response.json(cancelled ? joinOp('cancelled', { effect_state: 'none', blockers: [], allowed_actions: [] }) : joinOp('blocked'))
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Add node' }))
  fireEvent.change(screen.getByLabelText('New member name'), { target: { value: 'c4' } })
  fireEvent.change(screen.getByLabelText('Peer URL'), { target: { value: 'http://10.0.0.4:2380' } })
  fireEvent.click(screen.getByRole('button', { name: 'Request join' }))
  expect(await screen.findByText('the answer was lost on the way')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Request join' }))
  const dialog = await screen.findByRole('dialog')
  expect(await within(dialog).findByText(/Run this exact command on the new host/)).toBeInTheDocument()
  expect(within(dialog).getByText('shunt-control join --name c4 --data-dir <data-dir> --peer-url http://10.0.0.4:2380 --api 10.0.0.4:9901 --existing http://c1:9901 --resume join-1')).toBeInTheDocument()
  expect(within(dialog).getByText(/waiting for c4 to fetch its bootstrap/)).toBeInTheDocument()
  expect(posts).toHaveLength(2)
  expect(posts[0].body).toEqual({ name: 'c4', peer_url: 'http://10.0.0.4:2380' })
  expect(posts[0].key).toMatch(/^[0-9a-f]{32}$/)
  expect(posts[1].key).toBe(posts[0].key)
  expect(urls.some((u) => u.includes('/bootstrap'))).toBe(false)

  fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel join' }))
  expect(await screen.findByText('control-join cancellation requested; the operation is blocked')).toBeInTheDocument()
  expect(screen.queryByText(/Join of c4 cancelled/)).toBeNull()
  expect(await screen.findAllByText('Join of c4 cancelled; the membership is as it was', {}, { timeout: 4000 })).not.toHaveLength(0)
})

// A removal names the member by ID from a dry run: the answering node offers none; an unnamed
// learner is removed by its ID with the dry run's token, and what the removal leaves is shown first.
test('removes an unnamed learner by its ID after a dry run, and never offers to remove this node', async () => {
  let removed: { url: string; body: unknown } | null = null
  mock((url, init) => {
    if (url.endsWith('/v1/control')) return Response.json(threeMembers)
    if (url.endsWith('/v1/control/members/by-id/beef?dry_run=1') && init?.method === 'DELETE') return Response.json({ allowed: true, member: { id: 'beef', peer_urls: ['http://peer-9'], role: 'learner', started: false }, voters_after: 2, token: 'tok-beef', expires_at: '2026-09-28T12:10:00Z' })
    if (url.endsWith('/v1/control/members/by-id/beef') && init?.method === 'DELETE') { removed = { url, body: JSON.parse(String(init.body)) }; return Response.json({ member_id: 'beef', voters: 2, operation: 'rm-1' }) }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  expect(screen.getByText('this node')).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Remove member c1' })).toBeNull()
  expect(screen.getByRole('button', { name: 'Remove member c2' })).toBeEnabled()
  fireEvent.click(screen.getByRole('button', { name: 'Remove member beef' }))
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText('Remove (unnamed) (beef)')).toBeInTheDocument()
  expect(within(dialog).getByText(/a learner does not vote: 2 voting member\(s\) either way/)).toBeInTheDocument()
  expect(within(dialog).getByText(/names member beef and the member list as it is now/)).toBeInTheDocument()
  fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm' }))
  expect(await screen.findByText('member beef removed; 2 voting member(s) left')).toBeInTheDocument()
  expect(removed).toEqual({ url: expect.stringMatching(/\/v1\/control\/members\/by-id\/beef$/), body: { token: 'tok-beef' } })
})

// A refused removal says why and cannot be confirmed; one the server answers 202 is followed by its
// record, and every Remove waits while it runs.
test('shows a refused removal, and follows an accepted one until it ends', async () => {
  let ended = false
  const removing = (status: string) => ({ id: 'rm-2', kind: 'control-remove', actor: 'token:abc', status, phase: 'member_remove', scope: { resource: 'control:members', generation: 2 }, args: { id: '2' }, result: status === 'succeeded' ? { member_id: '2', name: 'c2', voters: 1 } : undefined, allowed_actions: [] })
  mock((url, init) => {
    if (url.endsWith('/v1/control')) return Response.json(threeMembers)
    if (url.endsWith('/v1/control/members/by-id/1?dry_run=1')) return Response.json({ allowed: false, reason: 'refused: member 1 (c1) is the control node answering this request', member: { id: '1', name: 'c1', peer_urls: ['http://peer-1'], role: 'voter', started: true }, voters_after: 1 })
    if (url.endsWith('/v1/control/members/by-id/2?dry_run=1')) return Response.json({ allowed: true, member: { id: '2', name: 'c2', peer_urls: ['http://peer-2'], role: 'voter', started: true }, voters_after: 1, warning: 'one voting member left: the control plane has no redundancy; join two more control nodes', token: 'tok-2' })
    if (url.endsWith('/v1/control/members/by-id/2') && init?.method === 'DELETE') return Response.json(removing('running'), { status: 202 })
    if (url.endsWith('/v1/operations/rm-2')) { const op = removing(ended ? 'succeeded' : 'running'); ended = true; return Response.json(op) }
  })
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Remove member c2' }))
  let dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText(/one voting member left/)).toBeInTheDocument()
  expect(within(dialog).getByText(/1 voting member\(s\); writes need 1/)).toBeInTheDocument()
  fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm' }))
  const section = await screen.findByRole('region', { name: 'Membership change' })
  expect(within(section).getByText('Removal of member 2')).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Remove member beef' })).toBeDisabled()
  expect(await within(section).findByText('member c2 removed', {}, { timeout: 4000 })).toBeInTheDocument()
  expect(await screen.findAllByText('member c2 removed')).toHaveLength(2) // the section and its toast

  fireEvent.click(within(section).getByRole('button', { name: 'Dismiss' }))
  // c1 is this node here, so ask as if from another node's view: the server's refusal is shown.
  mock((url) => {
    if (url.endsWith('/v1/control')) return Response.json({ ...threeMembers, node: 'c9' })
    if (url.endsWith('/v1/control/members/by-id/1?dry_run=1')) return Response.json({ allowed: false, reason: 'refused: member 1 (c1) is the control node answering this request', member: { id: '1', name: 'c1', peer_urls: ['http://peer-1'], role: 'voter', started: true }, voters_after: 1 })
  })
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }))
  fireEvent.click(await screen.findByRole('button', { name: 'Remove member c1' }))
  dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByRole('alert')).toHaveTextContent('is the control node answering this request')
  expect(within(dialog).getByRole('button', { name: 'Confirm' })).toBeDisabled()
})

// After a reload the screen finds the membership change from the control node's status, not from
// anything the browser kept: its progress, the new host's command and its cancel are there, and no
// other membership change can be started beside it.
test('follows the active membership change after a reload', async () => {
  mock((url) => {
    if (url.endsWith('/v1/control')) return Response.json({ ...threeMembers, active_membership_operation: 'join-1' })
    if (url.endsWith('/v1/operations/join-1')) return Response.json(joinOp('blocked'))
  })
  render(<StoreProvider><App /></StoreProvider>)
  const section = await screen.findByRole('region', { name: 'Membership change' })
  expect(await within(section).findByText(/--resume join-1$/)).toBeInTheDocument()
  expect(within(section).getByRole('button', { name: 'Cancel join' })).toBeEnabled()
  expect(screen.getByRole('button', { name: 'Remove member c2' })).toBeDisabled()
  fireEvent.click(screen.getByRole('button', { name: 'Add node' }))
  expect(screen.getByRole('button', { name: 'Request join' })).toBeDisabled()
  expect(screen.getByText('A membership change is in progress; one runs at a time.')).toBeInTheDocument()
})
