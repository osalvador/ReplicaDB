import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import LoadingState from './LoadingState';

describe('LoadingState', () => {
  it('exposes a stable live status label and visible loading copy', () => {
    render(<LoadingState label="Loading jobs" />);

    expect(screen.getByRole('status', { name: 'Loading jobs' })).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText('Loading jobs')).toBeInTheDocument();
  });
});
