import { byCluster, latencyChartData, scalarChartData, statusSeries } from './telemetryData'
import type { TelemetryPoint } from './api/client'

test('passes emitted percentiles to chart data without browser math', () => {
  const points: TelemetryPoint[] = [{
    start: '2026-09-22T12:00:00Z', end: '2026-09-22T12:00:10Z', series: 'client_total', op: 'all',
    count: 17, p50_us: 101, p90_us: 202, p99_us: 303, p999_us: 404, max_us: 505,
  }]
  expect(latencyChartData(points)).toEqual([{
    end: points[0].end, p50_us: 101, p90_us: 202, p99_us: 303, p999_us: 404,
  }])
})

test('passes control-node counter rates to chart data unchanged', () => {
  const end = '2026-09-22T12:00:10Z'
  const input = {
    requests_per_second: [{ start: '2026-09-22T12:00:00Z', end, series: 'requests_per_second', op: 'all', value: 12.75 }],
    bytes_out_per_second: [{ start: '2026-09-22T12:00:00Z', end, series: 'bytes_out_per_second', op: 'all', value: 4096.5 }],
  }
  expect(scalarChartData(input, ['requests_per_second', 'bytes_out_per_second'])).toEqual([{
    end, requests_per_second: 12.75, bytes_out_per_second: 4096.5,
  }])
})

test('shows each backend cluster share of requests, and says when a compared cluster has no traffic', async () => {
  const { render, screen, fireEvent } = await import('@testing-library/react')
  const { App } = await import('./App')
  const { StoreProvider } = await import('./store')
  const cluster = (name: string) => ({ name, type: 'minio', scheme: 'http', region: 'us-east-1', endpoints: [`${name}:9000`], access_key: 'AK', secret_ref: `control:${name}`, conditional_write: true, conditional_delete: false, references: [], read_only: false, reject_writes: false })
  const control = { node: 'c1', version: 't', directory: 3, directory_loaded: true, last_compaction: null, join: '', fleet: [],
    cluster: { members: [{ name: 'c1', id: '1', peer_urls: [], leader: true, started: true }], quorum: 1, started: 1, has_quorum: true, revision: 3, db_bytes: 1, db_in_use_bytes: 1, quota_bytes: 2, leader: 'c1' } }
  const end = new Date().toISOString()
  sessionStorage.setItem('shunt.control.token', 'actor-token')
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
    const url = String(input)
    if (url.endsWith('/v1/control')) return Response.json(control)
    if (url.endsWith('/v1/fleet')) return Response.json({ version: 3, members: [] })
    if (url.endsWith('/v1/status?all=1')) return Response.json({ version: 3, clusters: [cluster('minio-a'), cluster('minio-b')], placements: [
      { key: 'default/data01', state: 'ACTIVE', primary: '', names: null, ramp_writes: {}, fallback_reads: 0, dual_deletes: {}, read_only: false, reject_writes: false, per_bucket_telemetry: true },
    ] })
    if (url.endsWith('/v1/events')) return new Response('', { headers: { 'Content-Type': 'text/event-stream' } })
    if (url.includes('/v1/telemetry/series')) {
      const q = new URL(url, 'http://x').searchParams
      if (q.get('scope') === 'bucket:default/data01') {
        return Response.json({ points: [['minio-a', 5], ['minio-b', 3], ['minio-c', 2]].map(([cl, v]) => ({ start: end, end, series: q.get('series'), op: 'all', cluster: cl, value: v })) })
      }
      const value = q.get('series') !== 'requests_per_second' ? 0 : q.get('scope') === 'cluster:minio-a' ? 30 : q.get('scope') === 'cluster:minio-b' ? 10 : 40
      if (q.get('scope') === 'cluster:minio-b' && q.get('series') === 'client_total') return Response.json({ points: [] })
      return Response.json({ points: [{ start: end, end, series: q.get('series'), op: 'all', value, count: 1, p99_us: 100 }] })
    }
    return Response.json({ message: `unhandled ${url}` }, { status: 404 })
  })
  try {
    render(<StoreProvider><App /></StoreProvider>)
    fireEvent.click(await screen.findByRole('button', { name: 'Telemetry' }))
    expect(await screen.findByText('minio-a 75% · minio-b 25%')).toBeInTheDocument()
    expect(await screen.findByText(/No traffic reached minio-b in this window/)).toBeInTheDocument()
    // A bucket spread over three backends draws three: the share follows whatever clusters it has.
    expect(await screen.findByText('minio-a 50% · minio-b 30% · minio-c 20%')).toBeInTheDocument()
  } finally {
    sessionStorage.clear()
    vi.restoreAllMocks()
  }
})

test('draws one line per status code the scope answered, a read 404 last and named as not an error', () => {
  const at = (end: string, code: string, value: number) => ({ start: end, end, series: 'status_per_second', op: 'all', code, value })
  const points = [at('t1', '503', 2), at('t1', 'not_found', 9), at('t1', '403', 1), at('t2', '503', 4), at('t2', '0', 1), at('t2', '5xx', 1)]
  const { series, names } = statusSeries(points)
  expect(names).toEqual(['no response', '403', '503', 'other 5xx', '404 on read (not an error)'])
  expect(scalarChartData(series, names)).toEqual([
    { end: 't1', '503': 2, '403': 1, '404 on read (not an error)': 9 },
    { end: 't2', '503': 4, 'no response': 1, 'other 5xx': 1 },
  ])
})

test('groups a bucket scope into one series per backend cluster', () => {
  const p = (cluster: string, value: number) => ({ start: 't', end: 't', series: 'requests_per_second', op: 'all', cluster, value })
  const { series, names } = byCluster([p('b', 1), p('a', 2), p('c', 3), p('a', 4)])
  expect(names).toEqual(['a', 'b', 'c'])
  expect(series.a.map((x) => x.value)).toEqual([2, 4])
})
