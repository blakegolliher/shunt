import { parseSSEFrame, startOperation, unfinished } from './client'

test('parses authenticated stream frames without putting the token in a URL', () => {
  expect(parseSSEFrame('id: node:7\nevent: fleet\ndata: {"id":"proxy-a","event":"live"}')).toEqual({
    id: 'node:7',
    type: 'fleet',
    data: { id: 'proxy-a', event: 'live' },
  })
})

test('keeps forward-compatible text event data', () => {
  expect(parseSSEFrame('event: future\ndata: plain text')).toMatchObject({ type: 'future', data: 'plain text' })
})

test('treats pending, running and blocked operations as unfinished', () => {
  for (const status of ['pending', 'running', 'blocked'] as const) expect(unfinished({ status })).toBe(true)
  for (const status of ['succeeded', 'failed', 'cancelled'] as const) expect(unfinished({ status })).toBe(false)
  expect(unfinished(null)).toBe(false)
})

test('sends every change with its own Idempotency-Key and every read without one', async () => {
  const seen: (string | null)[] = []
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (_input, init) => {
    seen.push(new Headers(init?.headers).get('Idempotency-Key'))
    return Response.json({ id: 'op', status: 'pending' }, { status: 202 })
  })
  await startOperation('t', 'ramp', 'acme/data', { ratio: 0.5 })
  await startOperation('t', 'ramp', 'acme/data', { ratio: 0.5 })
  expect(seen[0]).toMatch(/^[0-9a-f-]{36}$/)
  expect(seen[1]).not.toBe(seen[0])
  vi.restoreAllMocks()
})
