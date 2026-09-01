import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import RunLogViewer from './RunLogViewer';

describe('RunLogViewer', () => {
  it('preserves multiline logs and reports truncation metadata', () => {
    render(<RunLogViewer log={{ runId: 'run-1', content: 'ERROR\n  at worker', truncated: true, capturedSize: 262200, formatVersion: 1 }} />);

    expect(screen.getByText(/ERROR/).textContent).toBe('ERROR\n  at worker');
    expect(screen.getByRole('alert')).toHaveTextContent('truncated');
    expect(screen.getByText(/262200 bytes/)).toBeInTheDocument();
  });

  it('renders empty, loading, and error states', () => {
    const { rerender } = render(<RunLogViewer />);
    expect(screen.getByText('No detailed log available.')).toBeInTheDocument();
    rerender(<RunLogViewer loading />);
    expect(screen.getByText('Loading run log')).toBeInTheDocument();
    rerender(<RunLogViewer error />);
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to load the run log.');
  });
});
