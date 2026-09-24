import type { TelemetryPoint } from './api/client'

export function latencyChartData(points: TelemetryPoint[]) {
  return points.map((point) => ({
    end: point.end,
    p50_us: point.p50_us ?? 0,
    p90_us: point.p90_us ?? 0,
    p99_us: point.p99_us ?? 0,
    p999_us: point.p999_us ?? 0,
  }))
}

export function scalarChartData(series: Record<string, TelemetryPoint[]>, names: readonly string[]) {
  const rows = new Map<string, Record<string, string | number>>()
  for (const name of names) {
    for (const point of series[name] ?? []) {
      const row = rows.get(point.end) ?? { end: point.end }
      row[name] = point.value ?? 0
      rows.set(point.end, row)
    }
  }
  return [...rows.values()].sort((a, b) => String(a.end).localeCompare(String(b.end)))
}

// statusLabel names a status key for a chart legend.
export function statusLabel(code: string) {
  switch (code) {
    case 'not_found': return '404 on read (not an error)'
    case '0': return 'no response'
    case '4xx': return 'other 4xx'
    case '5xx': return 'other 5xx'
    default: return code
  }
}

// statusSeries groups status_per_second points into one series per status seen, in code order
// with a read's 404 last, so the chart grows a line for whatever codes the fleet answered.
export function statusSeries(points: TelemetryPoint[]) {
  const series: Record<string, TelemetryPoint[]> = {}
  for (const point of points) {
    const name = statusLabel(point.code ?? '?')
    ;(series[name] ??= []).push(point)
  }
  const order = (name: string) => (name.startsWith('404 on read') ? 'z' : name.startsWith('no response') ? '0' : name)
  const names = Object.keys(series).sort((a, b) => order(a).localeCompare(order(b)))
  return { series, names }
}
