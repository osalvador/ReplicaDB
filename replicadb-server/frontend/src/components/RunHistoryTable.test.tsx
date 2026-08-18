import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as runsApi from '../api/runsApi';
import RunHistoryTable, { statusChipColors } from './RunHistoryTable';

vi.mock('../api/runsApi', () => ({
  listJobRuns: vi.fn()
}));

const mockedRunsApi = vi.mocked(runsApi);
const statuses = [
  'PENDING',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCEL_REQUESTED',
  'CANCELLED',
  'RETRY_SCHEDULED'
] as const;

function renderHistory(status: typeof statuses[number]) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedRunsApi.listJobRuns.mockResolvedValue({
    content: [{
      id: 'run-1',
      jobDefinitionId: 'job-1',
      previousRunId: null,
      status,
      attempt: 1,
      executorIdentity: 'api',
      leaseUntil: null,
      heartbeatAt: null,
      createdAt: '2026-08-18T10:00:00Z',
      startedAt: '2026-08-18T10:00:01Z',
      finishedAt: '2026-08-18T10:00:02Z',
      rowsProcessed: 10,
      durationMillis: 1000,
      committedWatermark: null,
      errorMessage: null,
      cancellationWarning: null
    }],
    page: 0,
    size: 50,
    totalElements: 1
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <RunHistoryTable jobId="job-1" />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('RunHistoryTable', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each(statuses)('renders %s with its fixed status color', async status => {
    renderHistory(status);

    const chipLabel = await screen.findByText(status);
    const chip = chipLabel.closest('.MuiChip-root');
    const color = statusChipColors[status];
    const colorClass = `MuiChip-color${color[0].toUpperCase()}${color.slice(1)}`;

    expect(chip).toHaveClass(colorClass);
    expect(screen.getByRole('link', { name: 'View run' })).toHaveAttribute('href', '/runs/run-1');
  });
});
