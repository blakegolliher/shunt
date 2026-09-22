export function Stat({ label, value, detail }: { label: string; value: string | number; detail?: string }) {
  return <div className="min-w-0">
    <p className="text-xs font-medium uppercase tracking-[0.16em] text-muted">{label}</p>
    <p className="mt-2 truncate text-2xl font-semibold text-paper">{value}</p>
    {detail && <p className="mt-1 text-sm text-muted">{detail}</p>}
  </div>
}
