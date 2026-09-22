const tones: Record<string, string> = {
  ACTIVE: 'border-ember-300/40 bg-ember-300/10 text-ember-300',
  RAMPING: 'border-ember-400/40 bg-ember-400/10 text-ember-400',
  MIGRATING: 'border-ember-500/50 bg-ember-500/15 text-[#FF9E61]',
  CUTOVER: 'border-ember-600/60 bg-ember-600/20 text-[#FFB080]',
}

export function StateBadge({ state }: { state: string }) {
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold tracking-wide ${tones[state] ?? 'border-ink-700 bg-ink-800 text-muted'}`}>{state}</span>
}
