import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as runsApi from '../api/runsApi';
import type { JobRunResponse } from '../api/runsApi';
import { getRunRefetchInterval, isTerminalRunStatus } from '../utils/runStatus';
import RunDetailPage from './RunDetailPage';

vi.mock('../api/runsApi', () => ({
  getRun: vi.fn(),
  getRunLog: vi.fn(),
  listJobRuns: vi.fn()
}));

const mockedRunsApi = vi.mocked(runsApi);

const baseRun: JobRunResponse = {
  id: 'run-1',
  jobDefinitionId: 'job-1',
  previousRunId: null,
  status: 'RUNNING',
  attempt: 1,
  executorIdentity: 'api',
  leaseUntil: null,
  heartbeatAt: null,
  createdAt: '2026-08-18T10:00:00Z',
  startedAt: '2026-08-18T10:00:01Z',
  finishedAt: null,
  rowsProcessed: 10,
  durationMillis: 1000,
  committedWatermark: '2026-08-18T09:59:00Z',
  errorMessage: null,
  cancellationWarning: null
};

function renderDetail(run: JobRunResponse = baseRun) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedRunsApi.getRun.mockResolvedValue(run);
  mockedRunsApi.getRunLog.mockResolvedValue({ runId: run.id, excerpt: 'run log excerpt' });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/runs/run-1']}>
        <Routes>
          <Route path="/runs/:id" element={<RunDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('RunDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('keeps polling running and failed statuses but stops for terminal statuses', () => {
    expect(isTerminalRunStatus('RUNNING')).toBe(false);
    expect(getRunRefetchInterval('RUNNING')).toBe(5000);
    expect(getRunRefetchInterval('FAILED')).toBe(5000);
    expect(getRunRefetchInterval('SUCCEEDED')).toBe(false);
    expect(getRunRefetchInterval('CANCELLED')).toBe(false);
    expect(getRunRefetchInterval('RETRY_SCHEDULED')).toBe(false);
  });

  it('renders counters, timings, watermark, and log excerpt', async () => {
    renderDetail();

    expect(await screen.findByText('RUNNING')).toBeInTheDocument();
    expect(screen.getByText('10')).toBeInTheDocument();
    expect(screen.getByText('1000 ms')).toBeInTheDocument();
    expect(screen.getByText('2026-08-18T09:59:00Z')).toBeInTheDocument();
    expect(await screen.findByText('run log excerpt')).toBeInTheDocument();
  });

  it('shows the persisted cancellation warning', async () => {
    renderDetail({
      ...baseRun,
      status: 'CANCEL_REQUESTED',
      cancellationWarning: 'The sink may be indeterminate.'
    });

    expect(await screen.findByRole('alert')).toHaveTextContent('The sink may be indeterminate.');
  });
});
