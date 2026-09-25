import { fireEvent, render, screen } from '@testing-library/react'
import { App } from './App'
import { StoreProvider } from './store'

const control = { node: 'c1', version: 't', directory: 3, directory_loaded: true, last_compaction: null, join: '', fleet: [],
  cluster: { members: [{ name: 'c1', id: '1', peer_urls: [], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 3, db_bytes: 1, db_in_use_bytes: 1, quota_bytes: 2, leader: 'c1' } }
const cluster = (name: string, references: string[]) => ({ name, type: 'minio', scheme: 'http', region: 'us-east-1', endpoints: [`${name}:9000`], access_key: 'AK', secret_ref: `control:${name}`,
  conditional_write: true, conditional_delete: false, references, read_only: false, reject_writes: false })

afterEach(() => { sessionStorage.clear(); vi.restoreAllMocks() })

// A cluster that is a tenant's default cannot be removed; its detail offers to change the default
// right there, instead of on a screen the operator would have to know about.
test('changes a tenant default from the cluster that blocks removal', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  const posted: string[] = []
  const clusters = [cluster('minio-b', ['tenants.default.default_cluster']), cluster('var204', ['placements.default/data02'])]
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    if (init?.method === 'POST') { posted.push(`${url} ${String(init.body)}`); return Response.json({ tenant: 'default', default_cluster: 'var204', version: 9 }) }
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 8, clusters, placements: [] })
    if (url.includes('/v1/clusters/') && url.endsWith('/view')) {
      const c = clusters.find((item) => url.includes(`/clusters/${item.name}/`)) ?? clusters[0]
      return Response.json({ ...c, capabilities: { conditional_write: { value: true, known: true }, conditional_delete: { value: false, known: true } }, probe: { reachable: true, latency_ms: 1, checked_at: '2026-09-24T12:00:00Z' } })
    }
    if (url.includes('/v1/telemetry/series')) return Response.json({ points: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Clusters' }))
  fireEvent.click(await screen.findByText('minio-b'))
  expect(await screen.findByText(/New buckets of tenant default are created on minio-b/)).toBeInTheDocument()
  expect(screen.getByLabelText('New default cluster for tenant default')).toHaveValue('var204')
  fireEvent.click(screen.getByRole('button', { name: "Make it tenant default's default" }))
  await screen.findByText("var204 is now tenant default's default cluster")
  expect(posted).toEqual(['/v1/tenants/default/default-cluster {"cluster":"var204"}'])
})

// The cluster detail shows where a secret rotation stands across the fleet.
test('shows a secret rotation installing across the fleet', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  const clusters = [cluster('var204', ['placements.default/data02'])]
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 8, clusters, placements: [] })
    if (url.includes('/v1/clusters/var204/view')) return Response.json({ ...clusters[0], capabilities: { conditional_write: { value: true, known: true }, conditional_delete: { value: false, known: true } },
      probe: { reachable: true, latency_ms: 1, checked_at: '2026-09-24T12:00:00Z' }, secret: { generation: '12', installed: ['proxy-a'], pending: ['proxy-b'], silent: [] } })
    if (url.includes('/v1/telemetry/series')) return Response.json({ points: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Clusters' }))
  fireEvent.click(await screen.findByText('var204'))
  expect(await screen.findByText(/Rotation installing: 1 proxy still on an older secret/)).toBeInTheDocument()
  expect(screen.getByText('generation 12')).toBeInTheDocument()
  expect(screen.getByText('proxy-b')).toBeInTheDocument()
})

// Rotating credentials from the cluster detail posts the new secret alone (the access key is kept
// when the field is empty) and says the new generation is installing.
test('rotates a cluster secret from its detail', async () => {
  sessionStorage.setItem('shunt.control.token', 't')
  const posted: string[] = []
  const clusters = [cluster('var204', ['placements.default/data02'])]
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input)
    if (init?.method === 'POST') { posted.push(`${url} ${String(init.body)}`); return Response.json({ name: 'var204', version: 13, generation: '13', cluster: clusters[0] }) }
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 8, clusters, placements: [] })
    if (url.includes('/v1/clusters/var204/view')) return Response.json({ ...clusters[0], capabilities: { conditional_write: { value: true, known: true }, conditional_delete: { value: false, known: true } },
      probe: { reachable: true, latency_ms: 1, checked_at: '2026-09-24T12:00:00Z' } })
    if (url.includes('/v1/telemetry/series')) return Response.json({ points: [] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  render(<StoreProvider><App /></StoreProvider>)
  fireEvent.click(await screen.findByRole('button', { name: 'Clusters' }))
  fireEvent.click(await screen.findByText('var204'))
  const rotate = await screen.findByRole('button', { name: 'Rotate credentials' })
  expect(rotate).toBeDisabled()
  fireEvent.change(screen.getByLabelText('New secret key'), { target: { value: 'new-secret' } })
  fireEvent.click(rotate)
  await screen.findByText('Credentials of var204 rotated')
  expect(await screen.findByText(/Secret generation 13 is installing/)).toBeInTheDocument()
  expect(posted).toEqual(['/v1/clusters/var204/credentials {"secret":"new-secret"}'])
})
