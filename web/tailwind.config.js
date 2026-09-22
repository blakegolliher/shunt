/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        ember: { 300: '#F08A4B', 400: '#E06A1F', 500: '#CC5500', 600: '#A64400' },
        ink: { 700: '#2A2A2A', 800: '#1F1F1F', 900: '#141414', 950: '#0B0B0B' },
        paper: '#F2ECE6',
        muted: '#9A928B',
      },
      boxShadow: {
        ember: '0 0 0 1px rgba(204,85,0,.3), 0 16px 48px rgba(0,0,0,.35)',
      },
    },
  },
  plugins: [],
}
