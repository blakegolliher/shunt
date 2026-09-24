import { position } from '../hashRange'
import type { HashRange } from '../hashRange'

// The key space of a spread bucket (ADR-0018): which leg owns each hash range, and the part moving.
export interface OwnedLeg { id: string; cluster: string; bucket: string; share: number; ranges: HashRange[]; idle?: boolean }
export interface MovingPart { from: string; to: string; range: HashRange; share: number }

const colors = ['bg-ember-500', 'bg-sky-500', 'bg-emerald-500', 'bg-violet-500', 'bg-amber-400', 'bg-rose-500', 'bg-teal-400', 'bg-indigo-400']
const legColor = (index: number) => colors[index % colors.length]
const pct = (v: number) => `${Math.round(v * 1000) / 10}%`

export function OwnershipBar({ legs, move, compact = false }: { legs: OwnedLeg[]; move?: MovingPart | null; compact?: boolean }) {
  const label = legs.map((l) => `${l.cluster}/${l.bucket} ${pct(l.share)}`).join(', ') + (move ? `; moving ${pct(move.share)} from ${move.from} to ${move.to}` : '')
  const moving = move ? position(move.range) : null
  return <div>
    <div role="img" aria-label={`Key space: ${label}`} className={`relative w-full overflow-hidden rounded-full bg-ink-800 ${compact ? 'h-2' : 'h-4'}`}>
      {legs.flatMap((l, i) => l.ranges.map((r) => { const p = position(r); return <span key={`${l.id}-${r.from}`} title={`${l.cluster}/${l.bucket}`} className={`absolute inset-y-0 ${legColor(i)}`} style={{ left: `${p.start * 100}%`, width: `${p.width * 100}%` }} /> }))}
      {moving && <span data-testid="moving-range" title={`moving from ${move?.from} to ${move?.to}`} className="absolute inset-y-0 border-x-2 border-paper bg-[repeating-linear-gradient(45deg,rgba(255,255,255,.55)_0,rgba(255,255,255,.55)_3px,transparent_3px,transparent_7px)]" style={{ left: `${moving.start * 100}%`, width: `${moving.width * 100}%` }} />}
    </div>
    {!compact && <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">{legs.map((l, i) => <li key={l.id} className="flex items-center gap-1.5"><span className={`inline-block h-2.5 w-2.5 rounded-full ${legColor(i)}`} /><span className="font-mono text-paper">{l.cluster}/{l.bucket}</span> {l.ranges.length ? pct(l.share) : l.idle ? 'idle' : 'none here'}{move?.from === l.id ? ' · moving out' : move?.to === l.id ? ' · moving in' : ''}</li>)}{move && <li className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full border border-paper bg-[repeating-linear-gradient(45deg,rgba(255,255,255,.7)_0,rgba(255,255,255,.7)_2px,transparent_2px,transparent_4px)]" />moving: {pct(move.share)} of keys</li>}</ul>}
  </div>
}
