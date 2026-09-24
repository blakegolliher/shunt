// Hash ranges of the key space (ADR-0018) are 64-bit and written as 16 hex digits; BigInt keeps
// them exact, which a JavaScript number cannot.
export interface HashRange { from: string; to: string }

export const fullRange: HashRange = { from: '0000000000000000', to: 'ffffffffffffffff' }

const hex = (v: bigint) => v.toString(16).padStart(16, '0')

// leadingShare is the first share (0..1) of a range, from its start: the keys a move of that share
// takes. A share of 1 is the whole range.
export function leadingShare(range: HashRange, share: number): HashRange {
  const from = BigInt('0x' + range.from)
  const to = BigInt('0x' + range.to)
  if (share >= 1) return range
  const width = to - from + 1n
  let end = from + (width * BigInt(Math.max(1, Math.round(share * 10000)))) / 10000n - 1n
  if (end < from) end = from
  return { from: range.from, to: hex(end) }
}

const space = 1n << 64n

// position is where a range sits in the key space, as fractions (0..1) for drawing: its start and
// its width. Four decimal digits of a fraction are exact enough for a bar, and BigInt keeps the
// arithmetic exact before that.
export function position(range: HashRange): { start: number; width: number } {
  const from = BigInt('0x' + range.from)
  const to = BigInt('0x' + range.to)
  const scale = 1_000_000n
  return { start: Number((from * scale) / space) / 1e6, width: Number(((to - from + 1n) * scale) / space) / 1e6 }
}
