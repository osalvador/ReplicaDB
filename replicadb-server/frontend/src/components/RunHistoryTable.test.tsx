import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as runsApi from '../api/runsApi';
import type { JobRunResponse } from '../api/runsApi';
import RunHistoryTable from './RunHistoryTable';
import { statusChipColors } from './StatusChip';
import { theme } from '../theme/theme';

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

function makeRun(overrides: Partial<JobRunResponse> = {}): JobRunResponse {
  return {
    id: 'run-1',
    jobDefinitionId: 'job-1',
    previousRunId: null,
    status: 'SUCCEEDED',
    attempt: 1,
    executorIdentity: 'api',
    leaseUntil: null,
    heartbeatAt: null,
    createdAt: '2026-08-18T10:00:00Z',
    availableAt: '2026-08-18T09:59:30Z',
    startedAt: '2026-08-18T10:00:01Z',
    finishedAt: '2026-08-18T10:00:02Z',
    rowsProcessed: 10,
    durationMillis: 1000,
    committedWatermark: null,
    errorMessage: null,
    cancellationWarning: null,
    ...overrides
  };
}

function renderHistoryWithRuns(content: JobRunResponse[]) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedRunsApi.listJobRuns.mockResolvedValue({
    content,
    page: 0,
    size: 50,
    totalElements: 1
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <Routes>
            <Route path="*" element={<RunHistoryTable jobId="job-1" />} />
            <Route path="/runs/:id" element={<div>Run destination</div>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

function renderHistory(status: typeof statuses[number]) {
  return renderHistoryWithRuns([makeRun({ status })]);
}

describe('RunHistoryTable', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each(statuses)('renders %s with its fixed status color', async status => {
    renderHistory(status);

    const chipLabel = await screen.findByText(status);
    const chip = screen.getByRole('status', { name: `Run status: ${status}` });
    const color = statusChipColors[status];

    expect(chip).toHaveTextContent(status);
    expect(chip).toHaveAttribute('data-status-color', color);
    expect(screen.getByRole('heading', { level: 2, name: 'Run history' })).toBeInTheDocument();
    expect(screen.getByText('2026-08-18T09:59:30Z')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'View run' })).toHaveAttribute('href', '/runs/run-1');
    expect(screen.getByText('1s')).toBeInTheDocument();
  });

  it('sorts runs by a selected column and exposes the active direction', async () => {
    renderHistoryWithRuns([
      makeRun({ id: 'run-older', status: 'FAILED', startedAt: '2026-08-18T09:00:00Z' }),
      makeRun({ id: 'run-newer', status: 'SUCCEEDED', startedAt: '2026-08-18T11:00:00Z' })
    ]);

    await screen.findByText('FAILED');
    const startedHeader = screen.getByRole('button', { name: 'Sort by Started' });
    expect(startedHeader).toHaveAttribute('aria-label', 'Sort by Started');
    expect(startedHeader.parentElement).toHaveAttribute('aria-sort', 'descending');

    fireEvent.click(startedHeader);

    const rows = screen.getAllByRole('link').filter(element => element.tagName === 'TR');
    expect(within(rows[0]).getByText('FAILED')).toBeInTheDocument();
    expect(startedHeader.parentElement).toHaveAttribute('aria-sort', 'ascending');
  });

  it('opens the run when its row is clicked', async () => {
    renderHistory('SUCCEEDED');

    const row = await screen.findByRole('link', { name: 'View run run-1' });
    fireEvent.click(row);

    expect(await screen.findByText('Run destination')).toBeInTheDocument();
  });

  it('opens the run when its row is activated from the keyboard', async () => {
    renderHistory('SUCCEEDED');

    const row = await screen.findByRole('link', { name: 'View run run-1' });
    fireEvent.keyDown(row, { key: 'Enter' });

    expect(await screen.findByText('Run destination')).toBeInTheDocument();
  });

  it('sends status and inclusive date filters to the server and resets them', async () => {
    renderHistoryWithRuns([]);

    await screen.findByRole('combobox', { name: 'Status' });
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Status' }));
    fireEvent.click(await screen.findByRole('option', { name: 'FAILED' }));
    fireEvent.click(await screen.findByRole('option', { name: 'RUNNING' }));
    fireEvent.change(screen.getByLabelText('From date'), { target: { value: '2026-08-01' } });
    fireEvent.change(screen.getByLabelText('To date'), { target: { value: '2026-08-31' } });

    await waitFor(() => expect(mockedRunsApi.listJobRuns).toHaveBeenLastCalledWith(
      'job-1',
      0,
      50,
      {
        status: ['FAILED', 'RUNNING'],
        from: '2026-08-01T00:00:00.000Z',
        to: '2026-09-01T00:00:00.000Z'
      }
    ));

    fireEvent.keyDown(screen.getByRole('listbox', { name: 'Status' }), { key: 'Escape' });
    const clearFiltersButton = screen.getByRole('button', { name: 'Clear filters' });
    fireEvent.click(clearFiltersButton);
    expect(clearFiltersButton).toBeDisabled();
    expect(screen.getByLabelText('From date')).toHaveValue('');
    expect(screen.getByLabelText('To date')).toHaveValue('');
  });

  it('removes one selected status with its close control', async () => {
    renderHistoryWithRuns([]);

    fireEvent.mouseDown(await screen.findByRole('combobox', { name: 'Status' }));
    fireEvent.click(await screen.findByRole('option', { name: 'FAILED' }));
    fireEvent.click(await screen.findByRole('option', { name: 'RUNNING' }));
    fireEvent.keyDown(screen.getByRole('listbox', { name: 'Status' }), { key: 'Escape' });
    fireEvent.click(screen.getByLabelText('Remove FAILED'));

    expect(screen.queryByLabelText('Remove FAILED')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Remove RUNNING')).toBeInTheDocument();
  });

  it('explains an invalid date range without requesting the server', async () => {
    renderHistoryWithRuns([]);

    await screen.findByLabelText('From date');
    fireEvent.change(screen.getByLabelText('From date'), { target: { value: '2026-09-02' } });
    fireEvent.change(screen.getByLabelText('To date'), { target: { value: '2026-09-01' } });

    expect(await screen.findByText('The start date must be on or before the end date.')).toBeInTheDocument();
    expect(mockedRunsApi.listJobRuns).toHaveBeenLastCalledWith(
      'job-1',
      0,
      50,
      { status: undefined, from: '2026-09-02T00:00:00.000Z', to: undefined }
    );
  });
});
