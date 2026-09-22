import { parseSSEFrame } from './client'

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
