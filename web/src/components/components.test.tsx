import { fireEvent, render, screen } from '@testing-library/react'
import { ConfirmDrawer } from './ConfirmDrawer'
import { FenceStatus } from './FenceStatus'
import { OwnershipBar } from './OwnershipBar'
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

test('the ownership bar draws the ranges of each leg and marks the part moving', () => {
  const legs = [
    { id: 'a', cluster: 'minio01', bucket: 'wide', share: 0.75, ranges: [{ from: '0000000000000000', to: '3fffffffffffffff' }, { from: '8000000000000000', to: 'ffffffffffffffff' }] },
    { id: 'b', cluster: 'minio02', bucket: 'wide-b', share: 0.25, ranges: [{ from: '4000000000000000', to: '7fffffffffffffff' }] },
    { id: 'c', cluster: 'minio02', bucket: 'wide-c', share: 0, ranges: [], idle: true },
  ]
  const move = { from: 'a', to: 'b', range: { from: '8000000000000000', to: 'bfffffffffffffff' }, share: 0.25 }
  const { container } = render(<OwnershipBar legs={legs} move={move} />)
  expect(screen.getByRole('img')).toHaveAccessibleName('Key space: minio01/wide 75%, minio02/wide-b 25%, minio02/wide-c 0%; moving 25% from a to b')
  const segments = container.querySelectorAll('[role="img"] > span[title^="minio"]')
  expect(Array.from(segments, (el) => (el as HTMLElement).style.left)).toEqual(['0%', '50%', '25%'])
  expect(Array.from(segments, (el) => (el as HTMLElement).style.width)).toEqual(['25%', '50%', '25%'])
  expect(screen.getByTestId('moving-range')).toHaveStyle({ left: '50%', width: '25%' })
  expect(screen.getByText('idle')).toBeInTheDocument()
  expect(screen.getByText('moving: 25% of keys', { exact: false })).toBeInTheDocument()
  expect(screen.getByText('· moving out', { exact: false })).toBeInTheDocument()
})
