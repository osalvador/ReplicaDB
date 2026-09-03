import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import RunLogViewer from './RunLogViewer';

describe('RunLogViewer', () => {
  it('preserves multiline logs and reports truncation metadata', () => {
    render(<RunLogViewer log={{
      runId: 'run-1',
      content: 'BEGIN\nERROR\n[TRUNCATED: middle omitted]\nEND',
      truncated: true,
      capturedSize: 262200,
      formatVersion: 1,
      capturedAt: '2026-09-03T06:00:00Z',
      updatedAt: '2026-09-03T06:01:00Z'
    }} />);

    expect(screen.getByRole('heading', { name: 'Beginning of captured log' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'End of captured log' })).toBeInTheDocument();
    expect(screen.getByRole('separator', { name: 'Middle of log omitted by server limit' })).toBeInTheDocument();
    expect(screen.getByLabelText('Beginning of captured log content')).toHaveTextContent('BEGIN');
    expect(screen.getByLabelText('End of captured log content')).toHaveTextContent('END');
    expect(screen.getByRole('alert')).toHaveTextContent('beginning and end of the output');
    expect(screen.getByRole('status', { name: 'Partial log' })).toBeInTheDocument();
    expect(screen.queryByText(/262200 bytes/)).not.toBeInTheDocument();
    expect(screen.getByText('Captured at')).toBeInTheDocument();
    expect(screen.queryByText('Updated at')).not.toBeInTheDocument();
  });

  it('renders a complete log as one captured block', () => {
    render(<RunLogViewer log={{ runId: 'run-1', content: 'INFO\nDone', truncated: false, capturedSize: 9 }} />);

    expect(screen.getByRole('heading', { name: 'Captured log' })).toBeInTheDocument();
    expect(screen.getByLabelText('Captured log content').textContent).toBe('INFO\nDone');
    expect(screen.getByRole('status', { name: 'Complete log' })).toBeInTheDocument();
    expect(screen.queryByRole('separator', { name: 'Middle of log omitted by server limit' })).not.toBeInTheDocument();
  });

  it('keeps a partial log honest when the server marker is missing', () => {
    render(<RunLogViewer log={{ runId: 'run-1', content: 'Available output', truncated: true, capturedSize: 300000 }} />);

    expect(screen.getByRole('heading', { name: 'Available captured log' })).toBeInTheDocument();
    expect(screen.queryByRole('separator', { name: 'Middle of log omitted by server limit' })).not.toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('did not provide the truncation boundary');
  });

  it('offers a retry action when loading the log fails', () => {
    const onRetry = vi.fn();
    render(<RunLogViewer error onRetry={onRetry} />);

    expect(screen.getByRole('alert')).toHaveTextContent('Check the server connection and try again.');
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it('disables the retry action while the log request is running again', () => {
    render(<RunLogViewer error onRetry={vi.fn()} retrying />);

    expect(screen.getByRole('button', { name: 'Retrying...' })).toBeDisabled();
  });

  it('preserves long and multilingual log content', () => {
    const content = `${'INFO '.repeat(200)}日本語 العربية 🚀`;
    render(<RunLogViewer log={{ runId: 'run-1', content, truncated: false, capturedSize: content.length }} />);

    expect(screen.getByLabelText('Captured log content').textContent).toBe(content);
  });

  it('copies the available redacted log content', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    render(<RunLogViewer log={{ runId: 'run-1', content: 'INFO\n[REDACTED]', truncated: false, capturedSize: 16 }} />);

    expect(screen.queryByRole('status', { name: 'Log action status' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Copy available log' }));

    await waitFor(() => expect(writeText).toHaveBeenCalledWith('INFO\n[REDACTED]'));
    expect(screen.getByRole('status', { name: 'Log action status' })).toHaveTextContent('Log copied to clipboard.');
    vi.unstubAllGlobals();
  });

  it('renders empty, loading, and error states', () => {
    const { rerender } = render(<RunLogViewer />);
    expect(screen.getByText('No log output was captured for this run.')).toBeInTheDocument();
    expect(screen.getByText('The server returned an empty log.')).toBeInTheDocument();
    rerender(<RunLogViewer loading />);
    expect(screen.getByText('Loading run log')).toBeInTheDocument();
    rerender(<RunLogViewer error />);
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to load the run log.');
  });
});
