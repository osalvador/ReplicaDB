import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as jobsApi from '../api/jobsApi';
import type { JobDefinitionResponse } from '../api/jobsApi';
import * as runsApi from '../api/runsApi';
import type { JobRunResponse } from '../api/runsApi';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import JobDetailPage from './JobDetailPage';

vi.mock('../api/jobsApi', () => ({
  deleteJob: vi.fn(),
  getJob: vi.fn(),
  listJobs: vi.fn(),
  updateJob: vi.fn()
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
  sourceDatasourceId: 'source-1',
  sourceDatasource: {
    id: 'source-1',
    name: 'Orders source',
    connectorType: 'postgres',
    safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/source'
  },
  sourceDatasourceUseEnabled: true,
  sourceTable: 'orders',
  sourceWhere: 'region = north',
  sinkDatasourceId: 'sink-1',
  sinkDatasource: {
    id: 'sink-1',
    name: 'Warehouse sink',
    connectorType: 'postgres',
    safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/sink'
  },
  sinkDatasourceUseEnabled: true,
  sinkTable: 'warehouse_orders',
  mode: 'complete-atomic',
  jobs: 4,
  incrementalWatermarkColumn: null,
  initialWatermarkValue: null,
  createdAt: '2026-08-18T10:00:00Z',
  updatedAt: '2026-08-18T11:00:00Z',
  modeWarning: null
};

function renderDetail(job: JobDefinitionResponse = baseJob, role: 'ADMIN' | 'OPERATOR' | 'VIEWER' = 'OPERATOR') {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  mockedJobsApi.getJob.mockResolvedValue(job);
  mockedRunsApi.listJobRuns.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });

  const view = render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          user: { id: 'user-id', username: role.toLowerCase(), role },
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <MemoryRouter initialEntries={['/jobs/job-1']}>
            <Routes>
              <Route path="/jobs/:id" element={<JobDetailPage />} />
              <Route path="/jobs" element={<div>Jobs destination</div>} />
              <Route path="/runs/:id" element={<div>Run destination</div>} />
            </Routes>
          </MemoryRouter>
        </AuthContext.Provider>
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
    expect(screen.getByText('Orders source (postgres)')).toBeInTheDocument();
    expect(screen.getByText('Warehouse sink (postgres)')).toBeInTheDocument();
    expect(screen.getAllByText('Enabled')).toHaveLength(2);
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('warehouse_orders')).toBeInTheDocument();
    expect(screen.getByText('complete-atomic')).toBeInTheDocument();
    expect(screen.getByText('4')).toBeInTheDocument();
    expect(screen.getAllByText('Not configured').length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText('2026-08-18T10:00:00Z')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Schedule card' })).toHaveTextContent('Schedule for job-1');
    expect(screen.getByRole('link', { name: 'Back to jobs' })).toHaveAttribute('href', '/jobs');
    expect(screen.getByRole('button', { name: 'Trigger run' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Edit' })).toHaveAttribute('href', '/jobs/job-1/edit');
    expect(screen.queryByRole('link', { name: 'Manage permissions' })).not.toBeInTheDocument();
  });

  it('shows the warning for complete mode', async () => {
    renderDetail({
      ...baseJob,
      mode: 'complete',
      modeWarning: 'Complete mode clears the sink before loading. If the run is interrupted or retried, the sink may be empty or partially populated. Use complete-atomic for an all-or-nothing load when supported.'
    });

    expect(await screen.findByText(/Use complete-atomic for an all-or-nothing load when supported/)).toBeInTheDocument();
  });

  it('does not show a warning for complete-atomic mode', async () => {
    renderDetail();

    await screen.findByRole('heading', { name: 'Orders replication' });
    expect(screen.queryByText('Complete mode clears the sink before loading.')).not.toBeInTheDocument();
  });

  it('allows admins to cancel or confirm job deletion', async () => {
    mockedJobsApi.deleteJob.mockResolvedValue();
    const { queryClient } = renderDetail(baseJob, 'ADMIN');
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    fireEvent.click(await screen.findByRole('button', { name: 'Delete job' }));
    const dialog = await screen.findByRole('dialog', { name: 'Delete job' });
    expect(dialog).toHaveTextContent('Orders replication');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Delete job' })).not.toBeInTheDocument());
    expect(mockedJobsApi.deleteJob).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Delete job' }));
    fireEvent.click(within(await screen.findByRole('dialog', { name: 'Delete job' }))
      .getByRole('button', { name: 'Delete job' }));
    expect(await screen.findByText('Jobs destination')).toBeInTheDocument();
    expect(mockedJobsApi.deleteJob).toHaveBeenCalledWith('job-1');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['jobs', 'job-1'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['jobs'] });
  });

  it('hides job deletion from non-admin users', async () => {
    renderDetail();

    await screen.findByRole('heading', { name: 'Orders replication' });
    expect(screen.queryByRole('button', { name: 'Delete job' })).not.toBeInTheDocument();
  });

  it('keeps the deletion dialog open and retryable after an API error', async () => {
    mockedJobsApi.deleteJob.mockRejectedValueOnce(
      new ApiError(409, 'Conflict', 'This job has an active run.')
    ).mockResolvedValueOnce();
    renderDetail(baseJob, 'ADMIN');

    fireEvent.click(await screen.findByRole('button', { name: 'Delete job' }));
    let dialog = await screen.findByRole('dialog', { name: 'Delete job' });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete job' }));

    expect(await within(await screen.findByRole('dialog', { name: 'Delete job' }))
      .findByText('This job has an active run.')).toBeInTheDocument();
    dialog = screen.getByRole('dialog', { name: 'Delete job' });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete job' }));
    expect(await screen.findByText('Jobs destination')).toBeInTheDocument();
    expect(mockedJobsApi.deleteJob).toHaveBeenCalledTimes(2);
  });

  it('renders advanced definition fields without exposing connection parameters', async () => {
    renderDetail({
      ...baseJob,
      sourceColumns: 'id, payload',
      sourceQuery: 'select id, payload from orders',
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
    expect(screen.getByText('staging.orders')).toBeInTheDocument();
    expect(screen.getByText('Disabled')).toBeInTheDocument();
    expect(screen.getAllByText('Enabled')).toHaveLength(4);
    expect(screen.getByText('250')).toBeInTheDocument();
    expect(screen.getByText('512')).toBeInTheDocument();
    expect(screen.queryByText('source_user')).not.toBeInTheDocument();
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

    expect(await screen.findByText('This job already has an active run.')).toBeInTheDocument();
    expect(screen.queryByText('Run destination')).not.toBeInTheDocument();
  });

  it('disables a datasource binding with a datasource-only update and invalidates the job', async () => {
    mockedJobsApi.updateJob.mockResolvedValue({ ...baseJob, sourceDatasourceUseEnabled: false });
    const { queryClient } = renderDetail();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    fireEvent.click((await screen.findAllByRole('button', { name: 'Disable binding' }))[0]);

    await waitFor(() => expect(mockedJobsApi.updateJob).toHaveBeenCalledWith(
      'job-1',
      expect.objectContaining({
        sourceDatasourceId: 'source-1',
        sourceDatasourceUseEnabled: false,
        sinkDatasourceId: 'sink-1',
        sinkDatasourceUseEnabled: true
      })
    ));
    const [, request] = mockedJobsApi.updateJob.mock.calls[0];
    expect(request).not.toHaveProperty('sourceConnect');
    expect(request).not.toHaveProperty('sinkConnect');
    expect(request).not.toHaveProperty('sourcePassword');
    await waitFor(() => expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['jobs', 'job-1'] }));
  });

  it('offers re-enable for disabled bindings and surfaces backend authorization errors', async () => {
    const disabledJob = {
      ...baseJob,
      sourceDatasourceUseEnabled: false,
      sinkDatasourceUseEnabled: false
    };
    mockedJobsApi.updateJob.mockRejectedValue(
      new ApiError(403, 'Forbidden', 'You do not have permission to re-enable this datasource binding.')
    );
    renderDetail(disabledJob);

    expect(await screen.findAllByRole('button', { name: 'Enable binding' })).toHaveLength(2);
    fireEvent.click(screen.getAllByRole('button', { name: 'Enable binding' })[0]);
    expect(await screen.findByText('You do not have permission to re-enable this datasource binding.'))
      .toBeInTheDocument();
  });

  it('shows the permissions action for admins', async () => {
    renderDetail(baseJob, 'ADMIN');

    expect(await screen.findByRole('link', { name: 'Manage permissions' })).toHaveAttribute(
      'href',
      '/jobs/job-1/permissions'
    );
  });
});
