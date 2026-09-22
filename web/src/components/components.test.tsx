import { fireEvent, render, screen } from '@testing-library/react'
import { ConfirmDrawer } from './ConfirmDrawer'
import { FenceStatus } from './FenceStatus'
import { StateBadge } from './StateBadge'

test('state and fence status keep operational states distinct', () => {
  render(<><StateBadge state="MIGRATING" /><FenceStatus held waitingOn={['proxy-b']} version={9} /></>)
  expect(screen.getByText('MIGRATING')).toBeInTheDocument()
  expect(screen.getByRole('status')).toHaveTextContent('proxy-b')
})

test('confirmation drawer is keyboard-addressable and separates destructive actions', () => {
  const confirm = vi.fn()
  render(<ConfirmDrawer open title="Purge source" summary="7 objects" destructive onClose={() => undefined} onConfirm={confirm} />)
  const button = screen.getByRole('button', { name: 'Confirm' })
  expect(screen.getByRole('dialog')).toHaveAttribute('aria-modal', 'true')
  expect(button.className).toContain('bg-red-600')
  fireEvent.click(button)
  expect(confirm).toHaveBeenCalledOnce()
})
