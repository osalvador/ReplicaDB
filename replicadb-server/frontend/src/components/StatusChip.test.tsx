import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { JobRunStatus } from '../api/runsApi';
import StatusChip, { statusChipColors } from './StatusChip';

const statuses = [
  'PENDING',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCEL_REQUESTED',
  'CANCELLED',
  'RETRY_SCHEDULED'
] as const satisfies readonly JobRunStatus[];

describe('StatusChip', () => {
  it.each(statuses)('renders %s with its semantic color and accessible label', status => {
    render(<StatusChip status={status} />);

    const chip = screen.getByRole('status', { name: `Run status: ${status}` });
    const color = statusChipColors[status];

    expect(chip).toHaveTextContent(status);
    expect(chip).toHaveAttribute('data-status-color', color);
  });

  it('renders unknown status as text instead of relying on color', () => {
    render(<StatusChip />);

    expect(screen.getByRole('status', { name: 'Run status: UNKNOWN' })).toHaveTextContent('UNKNOWN');
  });
});
