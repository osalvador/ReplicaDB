import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as jobsApi from '../api/jobsApi';
import type { JobDefinitionResponse } from '../api/jobsApi';
import * as runsApi from '../api/runsApi';
import JobDetailPage from './JobDetailPage';

vi.mock('../api/jobsApi', () => ({
  getJob: vi.fn(),
  listJobs: vi.fn()
}));

vi.mock('../api/runsApi', () => ({
  listJobRuns: vi.fn()
}));

const mockedJobsApi = vi.mocked(jobsApi);
const mockedRunsApi = vi.mocked(runsApi);

const baseJob: JobDefinitionResponse = {
  id: 'job-1',
  name: 'Orders replication',
  sourceConnect: 'jdbc:postgresql://source/db',
  sourceUser: 'source_user',
  sourceTable: 'orders',
  sourceWhere: 'region = north',
  sinkConnect: 'jdbc:postgresql://sink/db',
  sinkUser: 'sink_user',
  sinkTable: 'warehouse_orders',
  mode: 'complete-atomic',
  jobs: 4,
  incrementalWatermarkColumn: null,
  initialWatermarkValue: null,
  createdAt: '2026-08-18T10:00:00Z',
  updatedAt: '2026-08-18T11:00:00Z',
  sourcePasswordConfigured: true,
  sinkPasswordConfigured: true,
  modeWarning: null
};

function renderDetail(job: JobDefinitionResponse = baseJob) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedJobsApi.getJob.mockResolvedValue(job);
  mockedRunsApi.listJobRuns.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/jobs/job-1']}>
        <Routes>
          <Route path="/jobs/:id" element={<JobDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('JobDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the read-only definition fields', async () => {
    renderDetail();

    expect(await screen.findByRole('heading', { name: 'Orders replication' })).toBeInTheDocument();
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('warehouse_orders')).toBeInTheDocument();
    expect(screen.getByText('complete-atomic')).toBeInTheDocument();
    expect(screen.getByText('4')).toBeInTheDocument();
    expect(screen.getAllByText('Not configured')).toHaveLength(2);
    expect(screen.getByText('2026-08-18T10:00:00Z')).toBeInTheDocument();
  });

  it('shows the warning for complete mode', async () => {
    renderDetail({
      ...baseJob,
      mode: 'complete',
      modeWarning: 'An interrupted or retried complete run leaves the sink truncated or partially loaded.'
    });

    expect(await screen.findByRole('alert')).toHaveTextContent('sink truncated or partially loaded');
  });

  it('does not show a warning for complete-atomic mode', async () => {
    renderDetail();

    await screen.findByRole('heading', { name: 'Orders replication' });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
