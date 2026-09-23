import { fullRange, leadingShare } from './hashRange'

test('a leading share of a range is exact over the 64-bit key space', () => {
  expect(leadingShare(fullRange, 0.5)).toEqual({ from: '0000000000000000', to: '7fffffffffffffff' })
  expect(leadingShare(fullRange, 1)).toEqual(fullRange)
  expect(leadingShare({ from: '8000000000000000', to: 'ffffffffffffffff' }, 0.25)).toEqual({ from: '8000000000000000', to: '9fffffffffffffff' })
  expect(leadingShare({ from: '0000000000000010', to: '0000000000000013' }, 0.0001)).toEqual({ from: '0000000000000010', to: '0000000000000010' })
})
