import { latencyChartData, scalarChartData } from './telemetryData'
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
