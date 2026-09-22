import '@testing-library/jest-dom/vitest'

Object.defineProperty(navigator, 'clipboard', {
  value: { writeText: () => Promise.resolve() },
  configurable: true,
})
