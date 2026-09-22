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
