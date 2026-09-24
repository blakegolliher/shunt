import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { App } from './App'
import { StoreProvider, toastMillis, useStore } from './store'

const control = {
  node: 'c1', version: 'test', directory: 12, directory_loaded: true, last_compaction: null, join: 'shunt-control join --name <name>', fleet: [],
  cluster: { members: [
    { name: 'c1', id: '1', peer_urls: ['http://peer-1'], leader: true, started: true },
    { name: 'c2', id: '2', peer_urls: ['http://peer-2'], leader: false, started: true },
    { name: 'c3', id: '3', peer_urls: ['http://peer-3'], leader: false, started: true },
  ], quorum: 2, started: 3, has_quorum: true, revision: 42, db_bytes: 1048576, db_in_use_bytes: 524288, quota_bytes: 2147483648, leader: 'c1' },
}
const fleet = { version: 12, members: [
  { id: 'proxy-a', live: true, applied: 12, durable: 12, seq: 3, host: 'host-a', version: 'test' },
  { id: 'proxy-b', live: true, applied: 12, durable: 11, seq: 4, host: 'host-b', version: 'test' },
] }
const directory = { version: 12, clusters: [], placements: [] }

beforeEach(() => {
  sessionStorage.setItem('shunt.control.token', 'secret')
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json(fleet)
    if (url.endsWith('/v1/fleet/proxy-b')) return Response.json({ ...fleet.members[1], directory: 12, lineage: true, behind: 0, backpressure: false,
      lease: { granted: 3e9, seq: 4, age: 7e8, live: true },
      secrets: [{ cluster: 'vast01', want: '12', have: '11', current: false }], problems: ['restart cache at version 11, behind the 12 it serves'] })
    if (url.endsWith('/v1/status?all=1')) return Response.json(directory)
    if (url.endsWith('/v1/audit?limit=100')) return Response.json({ changes: [{ ts: '2026-09-22T12:00:00Z', actor: 'token:abc123def456', op: 'set-target', key: 'default/ui-demo', version: 12 }] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return new Response('', { status: 404 })
  })
})

afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

test('shows quorum and two live proxies from the control API', async () => {
  render(<StoreProvider><App /></StoreProvider>)
  expect(await screen.findByText(/quorum healthy/)).toBeInTheDocument()
  expect(screen.getByText('proxy-a')).toBeInTheDocument()
  expect(screen.getByText('proxy-b')).toBeInTheDocument()
  expect(screen.getByText('2 live')).toBeInTheDocument()
  // Installed and durable are shown apart: proxy-b's restart cache is a version behind.
  expect(screen.getByText('11 (not durable)')).toBeInTheDocument()
  await waitFor(() => expect(fetch).toHaveBeenCalledWith('/v1/control', expect.objectContaining({ headers: expect.any(Headers) })))
})

test('opens a proxy and shows what is off with its install', async () => {
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByText('proxy-b'))
  expect(await screen.findByText('restart cache at version 11, behind the 12 it serves')).toBeInTheDocument()
  expect(screen.getByText('generation 11 (control holds 12)')).toBeInTheDocument()
  // The lease as the control plane grants it (T07): the grant, the heartbeat it answered and its age.
  expect(screen.getByText(/grants 3 s per heartbeat; heartbeat 4 seen 0\.7 s ago/)).toBeInTheDocument()
})

test('shows the live audit tail instead of a placeholder screen', async () => {
  render(<StoreProvider><App /></StoreProvider>)
  await screen.findByText('Control members')
  fireEvent.click(screen.getByRole('button', { name: 'Audit' }))
  expect(await screen.findByText('token:abc123def456')).toBeInTheDocument()
  expect(screen.getByText('set-target')).toBeInTheDocument()
  expect(screen.getByText('default/ui-demo')).toBeInTheDocument()
})

test('names quorum loss and disables the fiction that control is healthy', async () => {
  vi.mocked(fetch).mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json({ ...control, cluster: { ...control.cluster, started: 1, has_quorum: false, leader: '' } })
    if (url.endsWith('/v1/fleet')) return Response.json(fleet)
    if (url.endsWith('/v1/status?all=1')) return Response.json(directory)
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return new Response('', { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  expect(await screen.findByText(/Control-plane quorum is lost/)).toBeInTheDocument()
  expect(screen.getByText(/quorum unavailable/)).toBeInTheDocument()
})

test('a success toast closes itself and a danger toast stays until clicked', async () => {
  let notify: ReturnType<typeof useStore>['notify'] = () => undefined
  function Notifier() { notify = useStore().notify; return null }
  render(<StoreProvider><App /><Notifier /></StoreProvider>)
  await screen.findByText('Control members')
  vi.useFakeTimers()
  try {
    act(() => { notify('Cluster garage added'); notify('refused: no', 'danger') })
    expect(screen.getByText('Cluster garage added')).toBeInTheDocument()
    act(() => { vi.advanceTimersByTime(toastMillis) })
    expect(screen.queryByText('Cluster garage added')).not.toBeInTheDocument()
    expect(screen.getByText('refused: no')).toBeInTheDocument()
    fireEvent.click(screen.getByText('refused: no'))
    expect(screen.queryByText('refused: no')).not.toBeInTheDocument()
  } finally {
    vi.useRealTimers()
  }
})
