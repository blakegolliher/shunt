import { render, screen, waitFor } from '@testing-library/react'
import { App } from './App'
import { StoreProvider } from './store'

const control = {
  node: 'c1', version: 'test', directory: 12, directory_loaded: true, last_compaction: null, join: 'shunt-control join --name <name>', fleet: [],
  cluster: { members: [
    { name: 'c1', id: '1', peer_urls: ['http://peer-1'], leader: true, started: true },
    { name: 'c2', id: '2', peer_urls: ['http://peer-2'], leader: false, started: true },
    { name: 'c3', id: '3', peer_urls: ['http://peer-3'], leader: false, started: true },
  ], quorum: 2, started: 3, has_quorum: true, revision: 42, db_bytes: 1048576, db_in_use_bytes: 524288, quota_bytes: 2147483648, leader: 'c1' },
}
const fleet = { version: 12, members: [
  { id: 'proxy-a', live: true, applied: 12, seq: 3, host: 'host-a', version: 'test' },
  { id: 'proxy-b', live: true, applied: 12, seq: 4, host: 'host-b', version: 'test' },
] }
const directory = { version: 12, clusters: [], placements: [] }

beforeEach(() => {
  sessionStorage.setItem('shunt.control.token', 'secret')
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json(fleet)
    if (url.endsWith('/v1/status?all=1')) return Response.json(directory)
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return new Response('', { status: 404 })
  })
})

afterEach(() => sessionStorage.clear())

test('shows quorum and two live proxies from the control API', async () => {
  render(<StoreProvider><App /></StoreProvider>)
  expect(await screen.findByText(/quorum healthy/)).toBeInTheDocument()
  expect(screen.getByText('proxy-a')).toBeInTheDocument()
  expect(screen.getByText('proxy-b')).toBeInTheDocument()
  expect(screen.getByText('2 live')).toBeInTheDocument()
  await waitFor(() => expect(fetch).toHaveBeenCalledWith('/v1/control', expect.objectContaining({ headers: expect.any(Headers) })))
})
