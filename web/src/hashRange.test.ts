import { fullRange, position } from './hashRange'

test('a range is placed in the key space exactly enough to draw', () => {
  expect(position(fullRange)).toEqual({ start: 0, width: 1 })
  expect(position({ from: '8000000000000000', to: 'bfffffffffffffff' })).toEqual({ start: 0.5, width: 0.25 })
  expect(position({ from: '0000000000000000', to: '7ffffffffffffffe' }).width).toBeCloseTo(0.5, 5)
})
