import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as runsApi from '../api/runsApi';
import type { JobRunResponse } from '../api/runsApi';
import { getRunRefetchInterval, isTerminalRunStatus } from '../utils/runStatus';
import { theme } from '../theme/theme';
import RunDetailPage from './RunDetailPage';

vi.mock('../api/runsApi', () => ({
  getRun: vi.fn(),
  getRunLog: vi.fn(),
  listJobRuns: vi.fn(),
  cancelRun: vi.fn(),
  retryRun: vi.fn()
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
  availableAt: '2026-08-18T09:59:30Z',
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

  const view = render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={['/runs/run-1']}>
          <Routes>
            <Route path="/runs/run-2" element={<div>Retry destination</div>} />
            <Route path="/runs/:id" element={<RunDetailPage />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );

  return { ...view, queryClient };
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
    expect(screen.getByRole('heading', { level: 2, name: 'Run metrics' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Log excerpt' })).toBeInTheDocument();
    expect(screen.getByRole('status', { name: 'Run status: RUNNING' })).toBeInTheDocument();
    expect(screen.getByText('10')).toBeInTheDocument();
    expect(screen.getByText('1000 ms')).toBeInTheDocument();
    expect(screen.getByText('2026-08-18T09:59:00Z')).toBeInTheDocument();
    expect(screen.getByText('2026-08-18T09:59:30Z')).toBeInTheDocument();
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

  const statuses = [
    ['RUNNING', true, false],
    ['PENDING', true, false],
    ['CANCEL_REQUESTED', true, false],
    ['SUCCEEDED', false, false],
    ['FAILED', false, true],
    ['CANCELLED', false, false],
    ['RETRY_SCHEDULED', false, false]
  ] as const;

  it.each(statuses)('renders actions for %s according to its status', async (status, canCancel, canRetry) => {
    renderDetail({ ...baseRun, status });

    await screen.findByText(status);
    if (canCancel) {
      expect(screen.getByRole('button', { name: 'Cancel run' })).toBeInTheDocument();
    } else {
      expect(screen.queryByRole('button', { name: 'Cancel run' })).not.toBeInTheDocument();
    }
    if (canRetry) {
      expect(screen.getByRole('button', { name: 'Retry run' })).toBeInTheDocument();
    } else {
      expect(screen.queryByRole('button', { name: 'Retry run' })).not.toBeInTheDocument();
    }
  });

  it('cancels a run, renders its warning, and invalidates the run query', async () => {
    mockedRunsApi.cancelRun.mockResolvedValue({ warning: 'The worker may still be stopping.' });
    const { queryClient } = renderDetail({ ...baseRun, status: 'RUNNING' });
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    fireEvent.click(await screen.findByRole('button', { name: 'Cancel run' }));

    await waitFor(() => expect(mockedRunsApi.cancelRun).toHaveBeenCalledWith('run-1'));
    expect(await screen.findByRole('alert')).toHaveTextContent('The worker may still be stopping.');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['runs', 'run-1'] });
  });

  it('retries a failed run and navigates to the new run', async () => {
    mockedRunsApi.retryRun.mockResolvedValue({ id: 'run-2' });
    renderDetail({ ...baseRun, status: 'FAILED' });

    fireEvent.click(await screen.findByRole('button', { name: 'Retry run' }));

    await waitFor(() => expect(mockedRunsApi.retryRun).toHaveBeenCalledWith('run-1'));
    expect(await screen.findByText('Retry destination')).toBeInTheDocument();
  });

  it('renders a cancel error without navigating', async () => {
    mockedRunsApi.cancelRun.mockRejectedValue(new ApiError(400, 'Cancel failed', 'Cancellation was rejected.'));
    renderDetail({ ...baseRun, status: 'RUNNING' });

    fireEvent.click(await screen.findByRole('button', { name: 'Cancel run' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Cancellation was rejected.');
    expect(screen.queryByText('Retry destination')).not.toBeInTheDocument();
  });

  it('renders a retry error without navigating', async () => {
    mockedRunsApi.retryRun.mockRejectedValue(new ApiError(400, 'Retry failed', 'Retry was rejected.'));
    renderDetail({ ...baseRun, status: 'FAILED' });

    fireEvent.click(await screen.findByRole('button', { name: 'Retry run' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Retry was rejected.');
    expect(screen.queryByText('Retry destination')).not.toBeInTheDocument();
  });
});
