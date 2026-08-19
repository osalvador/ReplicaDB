import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as jobsApi from '../api/jobsApi';
import type { JobDefinitionResponse } from '../api/jobsApi';
import * as runsApi from '../api/runsApi';
import type { JobRunResponse } from '../api/runsApi';
import { theme } from '../theme/theme';
import JobDetailPage from './JobDetailPage';

vi.mock('../api/jobsApi', () => ({
  getJob: vi.fn(),
  listJobs: vi.fn()
}));

vi.mock('../api/runsApi', () => ({
  listJobRuns: vi.fn(),
  triggerRun: vi.fn()
}));

vi.mock('../components/JobScheduleCard', () => ({
  default: (props: { jobId: string }) => <section aria-label="Schedule card">Schedule for {props.jobId}</section>
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

  const view = render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={['/jobs/job-1']}>
          <Routes>
            <Route path="/jobs/:id" element={<JobDetailPage />} />
            <Route path="/runs/:id" element={<div>Run destination</div>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );

  return { ...view, queryClient };
}

describe('JobDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the read-only definition fields', async () => {
    renderDetail();

    expect(await screen.findByRole('heading', { name: 'Orders replication' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Source' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Sink' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Execution' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Lifecycle' })).toBeInTheDocument();
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('warehouse_orders')).toBeInTheDocument();
    expect(screen.getByText('complete-atomic')).toBeInTheDocument();
    expect(screen.getByText('4')).toBeInTheDocument();
    expect(screen.getAllByText('Not configured').length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText('2026-08-18T10:00:00Z')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Schedule card' })).toHaveTextContent('Schedule for job-1');
    expect(screen.getByRole('link', { name: 'Back to jobs' })).toHaveAttribute('href', '/');
    expect(screen.getByRole('button', { name: 'Trigger run' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Edit' })).toHaveAttribute('href', '/jobs/job-1/edit');
  });

  it('shows the warning for complete mode', async () => {
    renderDetail({
      ...baseJob,
      mode: 'complete',
      modeWarning: 'Complete mode clears the sink before loading. If the run is interrupted or retried, the sink may be empty or partially populated. Use complete-atomic for an all-or-nothing load when supported.'
    });

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Use complete-atomic for an all-or-nothing load when supported.'
    );
  });

  it('does not show a warning for complete-atomic mode', async () => {
    renderDetail();

    await screen.findByRole('heading', { name: 'Orders replication' });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('renders advanced definition fields without exposing connection parameters', async () => {
    renderDetail({
      ...baseJob,
      sourceColumns: 'id, payload',
      sourceQuery: 'select id, payload from orders',
      sourceAuthMode: 'ActiveDirectoryDefault',
      sourceConnectionParams: { clientId: '[REDACTED]' },
      sinkColumns: 'payload, id',
      sinkStagingSchema: 'staging',
      sinkStagingTable: 'staging.orders',
      sinkDisableEscape: true,
      sinkDisableTruncate: false,
      fetchSize: 250,
      bandwidthThrottling: 512,
      verbose: true
    });

    expect(await screen.findByText('id, payload')).toBeInTheDocument();
    expect(screen.getByText('select id, payload from orders')).toBeInTheDocument();
    expect(screen.getByText('ActiveDirectoryDefault')).toBeInTheDocument();
    expect(screen.getByText('staging.orders')).toBeInTheDocument();
    expect(screen.getByText('Disabled')).toBeInTheDocument();
    expect(screen.getAllByText('Enabled')).toHaveLength(2);
    expect(screen.getByText('250')).toBeInTheDocument();
    expect(screen.getByText('512')).toBeInTheDocument();
    expect(screen.queryByText('clientId')).not.toBeInTheDocument();
  });

  it('triggers a run, invalidates run history, and navigates to the new run', async () => {
    mockedRunsApi.triggerRun.mockResolvedValue({ id: 'run-new' } as JobRunResponse);
    const { queryClient } = renderDetail();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    fireEvent.click(await screen.findByRole('button', { name: 'Trigger run' }));

    await waitFor(() => expect(mockedRunsApi.triggerRun).toHaveBeenCalledWith('job-1'));
    expect(await screen.findByText('Run destination')).toBeInTheDocument();
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['jobRuns', 'job-1'] });
  });

  it('disables the trigger button while the request is pending', async () => {
    let resolveTrigger: (value: JobRunResponse) => void = () => undefined;
    mockedRunsApi.triggerRun.mockReturnValue(new Promise(resolve => {
      resolveTrigger = resolve;
    }));
    renderDetail();

    const triggerButton = await screen.findByRole('button', { name: 'Trigger run' });
    fireEvent.click(triggerButton);
    expect(triggerButton).toBeDisabled();

    resolveTrigger({ id: 'run-new' });
    expect(await screen.findByText('Run destination')).toBeInTheDocument();
  });

  it('shows a conflict error without navigating when a run is already active', async () => {
    mockedRunsApi.triggerRun.mockRejectedValue(
      new ApiError(409, 'Active run', 'This job already has an active run.')
    );
    renderDetail();

    fireEvent.click(await screen.findByRole('button', { name: 'Trigger run' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('This job already has an active run.');
    expect(screen.queryByText('Run destination')).not.toBeInTheDocument();
  });
});
