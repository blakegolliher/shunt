import { useCallback, useEffect, useMemo, useState } from 'react'
import { Area, AreaChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { getTelemetrySeries } from '../api/client'
import type { TelemetryPoint } from '../api/client'
import { Card } from '../components/Card'
import { Stat } from '../components/Stat'
import { useStore } from '../store'
import { byCluster, latencyChartData, scalarChartData, statusSeries } from '../telemetryData'

const latencySeries = ['client_total', 'upstream_ttfb', 'upstream_total', 'proxy_overhead'] as const
const scalarSeries = ['requests_per_second', 'bytes_in_per_second', 'bytes_out_per_second'] as const
const colors = ['#F08A4B', '#4BA3F0', '#7BC96F', '#E0C04B', '#C06AE0', '#E0605A', '#5AD1C8', '#F2ECE6', '#CC5500', '#9A928B']
const inputClass = 'rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-sm text-paper'

function latest(points: TelemetryPoint[], field: 'p99_us' | 'value') {
  const point = points.at(-1)
  return point?.[field] ?? 0
}

function micros(value: number) {
  return value >= 1000 ? `${(value / 1000).toFixed(1)} ms` : `${Math.round(value)} µs`
}

function rate(value: number, suffix: string) {
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ${suffix}`
}

function timeTick(value: string) {
  return new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function ScalarChart({ label, data, names, area = false }: { label: string; data: Record<string, string | number>[]; names: readonly string[]; area?: boolean }) {
  const Chart = area ? AreaChart : LineChart
  return <div className="h-64" role="img" aria-label={label}>
    {data.length === 0 ? <div className="grid h-full place-items-center text-sm text-muted">Waiting for a completed telemetry window.</div> : <ResponsiveContainer width="100%" height="100%">
      <Chart data={data} syncId="telemetry"><CartesianGrid stroke="#2A2A2A" vertical={false} /><XAxis dataKey="end" tickFormatter={timeTick} stroke="#9A928B" fontSize={11} /><YAxis stroke="#9A928B" fontSize={11} /><Tooltip contentStyle={{ background: '#141414', border: '1px solid #2A2A2A' }} labelFormatter={(value) => new Date(String(value)).toLocaleString()} /><Legend />{names.map((name, index) => area
        ? <Area key={name} type="monotone" dataKey={name} stroke={colors[index % colors.length]} fill={colors[index % colors.length]} fillOpacity={0.18} isAnimationActive={false} />
        : <Line key={name} type="monotone" dataKey={name} stroke={colors[index % colors.length]} strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />)}</Chart>
    </ResponsiveContainer>}
  </div>
}

function LatencyChart({ name, points }: { name: string; points: TelemetryPoint[] }) {
  const data = latencyChartData(points)
  return <div className="rounded-lg border border-ink-700 bg-ink-950 p-3"><p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted">{name.replaceAll('_', ' ')}</p><ScalarChart label={`${name} emitted percentiles`} data={data} names={['p50_us', 'p90_us', 'p99_us', 'p999_us']} /></div>
}

export function Telemetry() {
  const { token, directory, fleet, lastEvent } = useStore()
  const clusters = useMemo(() => (directory?.clusters ?? []).map((cluster) => cluster.name), [directory])
  const proxies = useMemo(() => (fleet?.members ?? []).map((proxy) => proxy.id), [fleet])
  const scopes = useMemo(() => ['fleet', ...clusters.map((name) => `cluster:${name}`), ...proxies.map((id) => `proxy:${id}`)], [clusters, proxies])
  const [scope, setScope] = useState('fleet')
  const [minutes, setMinutes] = useState(15)
  const [op, setOp] = useState('all')
  const [series, setSeries] = useState<Record<string, TelemetryPoint[]>>({})
  const [compareA, setCompareA] = useState('')
  const [compareB, setCompareB] = useState('')
  const [comparison, setComparison] = useState<Record<string, TelemetryPoint[]>>({})
  const [backends, setBackends] = useState<Record<string, TelemetryPoint[]>>({})
  const [statuses, setStatuses] = useState<TelemetryPoint[]>([])
  const bucketOptions = useMemo(() => (directory?.placements ?? []).filter((p) => p.per_bucket_telemetry).map((p) => p.key), [directory])
  const [bucketChoice, setBucketChoice] = useState('')
  const [bucketMetric, setBucketMetric] = useState<'requests_per_second' | 'bytes_in_per_second' | 'bytes_out_per_second'>('requests_per_second')
  const [bucketPoints, setBucketPoints] = useState<TelemetryPoint[]>([])
  const bucketKey = bucketOptions.includes(bucketChoice) ? bucketChoice : bucketOptions[0] ?? ''
  const [error, setError] = useState('')
  const telemetryEvent = lastEvent?.type === 'telemetry' ? lastEvent.id : ''
  const effectiveScope = scopes.includes(scope) ? scope : 'fleet'
  const effectiveCompareA = compareA || clusters[0] || ''
  const effectiveCompareB = compareB || clusters[1] || ''

  const load = useCallback(async () => {
    const to = new Date()
    const from = new Date(to.getTime() - minutes * 60_000)
    const fetchOne = async (queryScope: string, name: string) => {
      const query = new URLSearchParams({ scope: queryScope, series: name, op, from: from.toISOString(), to: to.toISOString() })
      return (await getTelemetrySeries(token, query)).points
    }
    try {
      const names = [...latencySeries, ...scalarSeries]
      const values = await Promise.all(names.map((name) => fetchOne(effectiveScope, name)))
      setSeries(Object.fromEntries(names.map((name, index) => [name, values[index]])))
      setStatuses(await fetchOne(effectiveScope, 'status_per_second'))
      if (bucketKey) {
        const query = new URLSearchParams({ scope: `bucket:${bucketKey}`, series: bucketMetric, op: 'all', from: from.toISOString(), to: to.toISOString() })
        setBucketPoints((await getTelemetrySeries(token, query)).points)
      } else setBucketPoints([])
      const perCluster = await Promise.all(clusters.map((name) => fetchOne(`cluster:${name}`, 'requests_per_second')))
      setBackends(Object.fromEntries(clusters.map((name, index) => [name, perCluster[index]])))
      if (effectiveCompareA && effectiveCompareB) {
        const [a, b] = await Promise.all([fetchOne(`cluster:${effectiveCompareA}`, 'client_total'), fetchOne(`cluster:${effectiveCompareB}`, 'client_total')])
        setComparison({ [effectiveCompareA]: a, [effectiveCompareB]: b })
      } else setComparison({})
      setError('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause))
    }
  }, [bucketKey, bucketMetric, clusters, effectiveCompareA, effectiveCompareB, effectiveScope, minutes, op, token])

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), 0)
    return () => window.clearTimeout(timer)
  }, [load, telemetryEvent])

  const requestData = scalarChartData(series, ['requests_per_second'])
  const throughputData = scalarChartData(series, ['bytes_in_per_second', 'bytes_out_per_second'])
  const status = statusSeries(statuses)
  const bucket = byCluster(bucketPoints)
  const bucketData = scalarChartData(bucket.series, bucket.names)
  const bucketNow = bucket.names.map((name) => ({ name, value: latest(bucket.series[name] ?? [], 'value') }))
  const bucketTotal = bucketNow.reduce((sum, b) => sum + b.value, 0)
  const bucketShare = bucketTotal > 0 ? bucketNow.map((b) => `${b.name} ${Math.round((b.value / bucketTotal) * 100)}%`).join(' · ') : 'no traffic in the last window'
  const statusData = scalarChartData(status.series, status.names)
  const backendData = scalarChartData(backends, clusters)
  const backendNow = clusters.map((name) => ({ name, value: latest(backends[name] ?? [], 'value') }))
  const backendTotal = backendNow.reduce((sum, b) => sum + b.value, 0)
  const backendShare = backendTotal > 0 ? backendNow.map((b) => `${b.name} ${Math.round((b.value / backendTotal) * 100)}%`).join(' · ') : 'no requests in the last window'
  const compareData = useMemo(() => {
    const named: Record<string, TelemetryPoint[]> = {}
    for (const [name, points] of Object.entries(comparison)) named[name] = points.map((point) => ({ ...point, value: point.p99_us ?? 0 }))
    return scalarChartData(named, Object.keys(named))
  }, [comparison])
  const compareNames = Object.keys(comparison)
  const idle = compareNames.filter((name) => (comparison[name] ?? []).length === 0)
  const clusterP99 = compareNames.map((name) => `${name} ${micros(latest(comparison[name], 'p99_us'))}`).join(' · ') || 'choose two clusters'

  return <div className="space-y-5">
    <Card eyebrow="Emitted telemetry" title="Fleet performance" action={<span className="text-xs text-muted">SSE live · no polling</span>}>
      <div className="grid gap-3 sm:grid-cols-3"><label className="text-sm text-muted">Scope<select aria-label="Telemetry scope" value={effectiveScope} onChange={(event) => setScope(event.target.value)} className={`mt-2 w-full ${inputClass}`}>{scopes.map((value) => <option key={value}>{value}</option>)}</select></label><label className="text-sm text-muted">Window<select aria-label="Telemetry window" value={minutes} onChange={(event) => setMinutes(Number(event.target.value))} className={`mt-2 w-full ${inputClass}`}><option value={5}>5 minutes</option><option value={15}>15 minutes</option><option value={60}>60 minutes</option></select></label><label className="text-sm text-muted">Operation class<select aria-label="Operation class" value={op} onChange={(event) => setOp(event.target.value)} className={`mt-2 w-full ${inputClass}`}>{['all', 'read', 'write', 'list', 'delete', 'multipart', 'other'].map((value) => <option key={value}>{value}</option>)}</select></label></div>
      {error && <p role="alert" className="mt-4 rounded-lg border border-red-600 bg-red-950/40 p-3 text-sm text-red-100">{error}</p>}
    </Card>

    <Card title="Current window"><div className="grid gap-5 sm:grid-cols-2 xl:grid-cols-5"><Stat label="Client p99" value={micros(latest(series.client_total ?? [], 'p99_us'))} /><Stat label="Upstream p99" value={micros(latest(series.upstream_total ?? [], 'p99_us'))} detail={clusterP99} /><Stat label="Proxy overhead p99" value={micros(latest(series.proxy_overhead ?? [], 'p99_us'))} /><Stat label="Requests" value={rate(latest(series.requests_per_second ?? [], 'value'), '/s')} /><Stat label="Throughput" value={rate(latest(series.bytes_out_per_second ?? [], 'value'), 'B/s out')} detail={`${rate(latest(series.bytes_in_per_second ?? [], 'value'), 'B/s in')}`} /></div></Card>

    <div className="grid gap-5 xl:grid-cols-2"><Card title="Request rate" eyebrow={op}><ScalarChart label="Request rate by completed window" data={requestData} names={['requests_per_second']} area /></Card><Card title="Throughput" eyebrow={effectiveScope}><ScalarChart label="Bytes in and out per second" data={throughputData} names={['bytes_in_per_second', 'bytes_out_per_second']} /></Card></div>

    <Card eyebrow="Microseconds from the API" title="Latency percentiles"><div className="grid gap-4 xl:grid-cols-2">{latencySeries.map((name) => <LatencyChart key={name} name={name} points={series[name] ?? []} />)}</div></Card>

    <Card eyebrow={`${op} · requests per second`} title="Traffic by backend" action={<span className="text-xs text-muted">{backendShare}</span>}><ScalarChart label="Requests per second by backend cluster" data={backendData} names={clusters} /></Card>

    <Card eyebrow="Per bucket · by backend" title="Bucket traffic" action={<span className="text-xs text-muted">{bucketKey ? bucketShare : ''}</span>}>
      {bucketOptions.length === 0 ? <p className="text-sm text-muted">No bucket is counted by backend yet. Buckets spread over legs or moving are counted automatically; watch any other from its detail on Buckets, or with shunt watch.</p> : <>
        <div className="mb-4 grid gap-3 sm:grid-cols-2"><label className="text-sm text-muted">Bucket<select aria-label="Bucket for traffic by backend" value={bucketKey} onChange={(event) => setBucketChoice(event.target.value)} className={`mt-2 w-full ${inputClass}`}>{bucketOptions.map((key) => <option key={key}>{key}</option>)}</select></label><label className="text-sm text-muted">Measure<select aria-label="Bucket traffic measure" value={bucketMetric} onChange={(event) => setBucketMetric(event.target.value as typeof bucketMetric)} className={`mt-2 w-full ${inputClass}`}><option value="requests_per_second">requests per second</option><option value="bytes_in_per_second">bytes in per second</option><option value="bytes_out_per_second">bytes out per second</option></select></label></div>
        <ScalarChart label={`${bucketKey} ${bucketMetric} by backend cluster`} data={bucketData} names={bucket.names} />
      </>}
    </Card>

    <Card eyebrow={`${effectiveScope} · ${op} · per second`} title="Responses by status"><p className="mb-3 text-xs text-muted">Every response that was not a success, by status: one line per code the scope answered. A read answered 404 (a HEAD or GET of a key that is not there) is an answer, not an error, and has its own line; codes outside 400, 403, 404, 405, 409, 411, 412, 416, 429, 500–504 count as other 4xx or other 5xx.</p>{statuses.length === 0 ? <p className="text-sm text-muted">No error responses in this window.</p> : <ScalarChart label="Responses by status code" data={statusData} names={status.names} />}</Card>

    <Card eyebrow="Ramp hold judgment" title="Cluster comparison"><div className="mb-4 grid gap-3 sm:grid-cols-2"><label className="text-sm text-muted">First cluster<select aria-label="First comparison cluster" value={effectiveCompareA} onChange={(event) => setCompareA(event.target.value)} className={`mt-2 w-full ${inputClass}`}>{clusters.map((name) => <option key={name}>{name}</option>)}</select></label><label className="text-sm text-muted">Second cluster<select aria-label="Second comparison cluster" value={effectiveCompareB} onChange={(event) => setCompareB(event.target.value)} className={`mt-2 w-full ${inputClass}`}>{clusters.map((name) => <option key={name}>{name}</option>)}</select></label></div>{idle.length > 0 && <p className="mb-3 text-sm text-muted">No traffic reached {idle.join(' or ')} in this window, so there is nothing to compare yet: a ramp sends writes there only for keys it has moved.</p>}<ScalarChart label="Client total p99 cluster comparison" data={compareData} names={compareNames} /></Card>
  </div>
}
