import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as jobsApi from '../api/jobsApi';
import DashboardPage from './DashboardPage';

vi.mock('../api/jobsApi', () => ({
  listJobs: vi.fn()
}));

const mockedJobsApi = vi.mocked(jobsApi);

const jobs = [
  {
    id: 'job-1',
    name: 'Orders replication',
    sourceTable: 'orders',
    sinkTable: 'warehouse_orders',
    mode: 'complete',
    modeWarning: 'An interrupted or retried complete run leaves the sink truncated or partially loaded.'
  },
  {
    id: 'job-2',
    name: 'Customers replication',
    sourceTable: 'customers',
    sinkTable: 'warehouse_customers',
    mode: 'complete-atomic',
    modeWarning: null
  }
];

const fullPageJobs = Array.from({ length: 50 }, (_, index) => ({
  ...jobs[0],
  id: `job-${index}`,
  name: `Orders replication ${index}`
}));

function renderDashboard(response = { content: jobs, page: 0, size: 50, totalElements: 2 }) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedJobsApi.listJobs.mockResolvedValue(response);

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <DashboardPage />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('DashboardPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the visible job rows', async () => {
    renderDashboard();

    expect(await screen.findByText('Orders replication')).toBeInTheDocument();
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('warehouse_orders')).toBeInTheDocument();
    expect(screen.getByText('complete')).toBeInTheDocument();
  });

  it('shows a warning indicator only for jobs with modeWarning', async () => {
    renderDashboard();

    await screen.findByText('Orders replication');

    expect(screen.getByLabelText(/Job warning:/)).toBeInTheDocument();
    expect(screen.queryByLabelText('Job warning: null')).not.toBeInTheDocument();
  });

  it('requests the next page when pagination advances', async () => {
    renderDashboard({ content: fullPageJobs, page: 0, size: 50, totalElements: 100 });

    await screen.findByText('Orders replication 0');
    fireEvent.click(screen.getByRole('button', { name: 'Go to next page' }));

    await waitFor(() => expect(mockedJobsApi.listJobs).toHaveBeenCalledWith(1, 50));
  });

  it('disables next page when the response is short', async () => {
    renderDashboard({ content: jobs.slice(0, 1), page: 0, size: 50, totalElements: 100 });

    await screen.findByText('Orders replication');
    expect(screen.getByRole('button', { name: 'Go to next page' })).toBeDisabled();
  });

  it('disables next page for an empty result', async () => {
    renderDashboard({ content: [], page: 0, size: 50, totalElements: 0 });

    expect(await screen.findByText('No jobs available.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Go to next page' })).toBeDisabled();
  });
});
