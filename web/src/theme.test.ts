const palette = {
  ember300: '#F08A4B',
  paper: '#F2ECE6',
  muted: '#9A928B',
  ink950: '#0B0B0B',
  ink900: '#141414',
}

function luminance(hex: string): number {
  const values = hex.slice(1).match(/.{2}/g)!.map((part) => Number.parseInt(part, 16) / 255)
    .map((value) => value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4)
  return 0.2126 * values[0] + 0.7152 * values[1] + 0.0722 * values[2]
}

function contrast(a: string, b: string): number {
  const [bright, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (bright + 0.05) / (dark + 0.05)
}

test.each([
  ['paper on ink-950', palette.paper, palette.ink950],
  ['muted on ink-950', palette.muted, palette.ink950],
  ['ember-300 on ink-950', palette.ember300, palette.ink950],
  ['paper on ink-900', palette.paper, palette.ink900],
  ['muted on ink-900', palette.muted, palette.ink900],
])('%s has WCAG AA text contrast', (_name, foreground, background) => {
  expect(contrast(foreground, background)).toBeGreaterThanOrEqual(4.5)
})
