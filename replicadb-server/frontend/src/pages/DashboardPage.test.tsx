import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as dashboardApi from '../api/dashboardApi';
import DashboardPage from './DashboardPage';
import { theme } from '../theme/theme';

vi.mock('../api/dashboardApi', async () => {
  const actual = await vi.importActual<typeof import('../api/dashboardApi')>('../api/dashboardApi');
  return { ...actual, getDashboardSummary: vi.fn() };
});

const mockedDashboardApi = vi.mocked(dashboardApi);
const summary = {
  from: '2026-09-02T10:00:00Z',
  to: '2026-09-03T10:00:00Z',
  totalJobs: 11,
  activeRuns: 1,
  totalRuns: 4,
  succeededRuns: 3,
  failedRuns: 1,
  rowsProcessed: 1280,
  averageDurationMillis: 1500,
  averageLatencyMillis: 120,
  outcomes: [
    { bucket: '2026-09-03T09:00:00Z', succeeded: 3, failed: 1, active: 1 }
  ],
  jobPerformance: [{
    jobId: 'job-1',
    jobName: 'Orders replication',
    runCount: 4,
    rowsProcessed: 1280,
    averageDurationMillis: 1500,
    averageLatencyMillis: 120
  }]
};

function renderDashboard(response = summary) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  mockedDashboardApi.getDashboardSummary.mockResolvedValue(response);
  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter><DashboardPage /></MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('DashboardPage', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders operational metrics and performance bars', async () => {
    renderDashboard();

    expect(await screen.findByRole('heading', { name: 'Replication pulse' })).toBeInTheDocument();
    expect(screen.getByText('11')).toBeInTheDocument();
    expect(screen.getByText('75%')).toBeInTheDocument();
    expect(screen.getByText('1,280')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Run outcomes' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Job performance' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Last 24h' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('link', { name: 'Open jobs' })).toHaveAttribute('href', '/jobs');
  });

  it('refetches with a new window when a range preset is selected', async () => {
    renderDashboard();

    await screen.findByRole('heading', { name: 'Replication pulse' });
    fireEvent.click(screen.getByRole('button', { name: 'Last 7d' }));

    await waitFor(() => expect(mockedDashboardApi.getDashboardSummary).toHaveBeenCalledTimes(2));
    expect(screen.getByRole('button', { name: 'Last 7d' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('applies a custom two-hour window', async () => {
    renderDashboard();

    await screen.findByRole('heading', { name: 'Replication pulse' });
    fireEvent.click(screen.getByRole('button', { name: 'Custom time range' }));
    fireEvent.change(screen.getByLabelText('Duration'), { target: { value: '2' } });
    fireEvent.click(screen.getByRole('button', { name: 'Apply range' }));

    await waitFor(() => expect(mockedDashboardApi.getDashboardSummary).toHaveBeenCalledTimes(2));
    const customWindow = mockedDashboardApi.getDashboardSummary.mock.calls[1][0];
    expect(new Date(customWindow.to).getTime() - new Date(customWindow.from).getTime()).toBe(2 * 60 * 60 * 1000);
    expect(screen.getByRole('button', { name: 'Custom time range: Last 2h' })).toHaveTextContent('Last 2h');
  });

  it('offers retry when dashboard metrics cannot be loaded', async () => {
    mockedDashboardApi.getDashboardSummary
      .mockRejectedValueOnce(new Error('Network Error'))
      .mockResolvedValueOnce(summary);
    renderDashboard();

    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to load dashboard metrics.');
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(await screen.findByRole('heading', { name: 'Replication pulse' })).toBeInTheDocument();
  });

  it('shows a focused empty state when the selected window has no runs', async () => {
    renderDashboard({ ...summary, totalRuns: 0, succeededRuns: 0, failedRuns: 0, outcomes: [], jobPerformance: [] });

    expect(await screen.findByText('No runs in this window.')).toBeInTheDocument();
    expect(screen.getByText('No job performance data.')).toBeInTheDocument();
  });
});
