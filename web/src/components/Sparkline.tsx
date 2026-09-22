import { Line, LineChart, ResponsiveContainer } from 'recharts'

export function Sparkline({ values, label }: { values: number[]; label: string }) {
  const data = values.map((value, index) => ({ index, value }))
  return <div className="h-10 w-28" role="img" aria-label={label}>
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data}><Line type="monotone" dataKey="value" stroke="#E06A1F" strokeWidth={2} dot={false} isAnimationActive={false} /></LineChart>
    </ResponsiveContainer>
  </div>
}
