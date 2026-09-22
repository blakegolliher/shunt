import { useState } from 'react'

export function CopyLine({ value, label = 'Copy command' }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  const copy = async () => {
    await navigator.clipboard.writeText(value)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return <div className="flex items-center gap-2 rounded-lg border border-ink-700 bg-ink-950 p-2">
    <code className="min-w-0 flex-1 overflow-x-auto px-2 text-xs text-paper">{value}</code>
    <button type="button" aria-label={label} onClick={() => void copy()} className="shrink-0 rounded-md bg-ink-700 px-3 py-2 text-xs font-semibold text-paper hover:bg-ember-600">{copied ? 'Copied' : 'Copy'}</button>
  </div>
}
