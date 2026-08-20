import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useParams } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as jobsApi from '../api/jobsApi';
import type { JobDefinitionResponse } from '../api/jobsApi';
import { theme } from '../theme/theme';
import JobFormPage from './JobFormPage';

vi.mock('../api/jobsApi', async () => {
  const actual = await vi.importActual<typeof import('../api/jobsApi')>('../api/jobsApi');
  return {
    ...actual,
    createJob: vi.fn(),
    getJob: vi.fn(),
    updateJob: vi.fn()
  };
});

const mockedJobsApi = vi.mocked(jobsApi);

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
  mode: 'incremental',
  jobs: 4,
  incrementalWatermarkColumn: 'updated_at',
  initialWatermarkValue: '0',
  createdAt: '2026-08-18T10:00:00Z',
  updatedAt: '2026-08-18T11:00:00Z',
  sourcePasswordConfigured: true,
  sinkPasswordConfigured: true,
  maxAttempts: 5,
  retryBackoffSeconds: 90,
  automaticRetryEnabled: true,
  modeWarning: null
};

function SavedJob() {
  const { id } = useParams<{ id: string }>();
  return <div>Saved {id}</div>;
}

function renderForm(path: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="/jobs/new" element={<JobFormPage />} />
            <Route path="/jobs/:id/edit" element={<JobFormPage />} />
            <Route path="/jobs/:id" element={<SavedJob />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

function fillRequiredFields(includeSourceTable = true) {
  fireEvent.change(screen.getByLabelText(/^Name/), { target: { value: 'New job' } });
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Source data source type' }));
  fireEvent.click(screen.getByRole('option', { name: 'PostgreSQL' }));
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Sink data source type' }));
  fireEvent.click(screen.getAllByRole('option', { name: 'PostgreSQL' })[0]);
  const hosts = screen.getAllByLabelText('Host');
  const ports = screen.getAllByLabelText('Port');
  const databases = screen.getAllByLabelText('Database / SID or Service Name');
  fireEvent.change(hosts[0], { target: { value: 'source.example' } });
  fireEvent.change(ports[0], { target: { value: '5432' } });
  fireEvent.change(databases[0], { target: { value: 'source_db' } });
  fireEvent.change(hosts[1], { target: { value: 'sink.example' } });
  fireEvent.change(ports[1], { target: { value: '5432' } });
  fireEvent.change(databases[1], { target: { value: 'sink_db' } });
  if (includeSourceTable) {
    fireEvent.change(screen.getByLabelText(/^Table/), { target: { value: 'source_table' } });
  }
  fireEvent.change(screen.getByLabelText(/^Sink table/), { target: { value: 'sink_table' } });
  fireEvent.change(screen.getByLabelText(/^Parallel tasks/), { target: { value: '2' } });
}

describe('JobFormPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('creates a job with a normalized payload and navigates to the returned job', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    expect(screen.getByRole('heading', { level: 2, name: 'Basics' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Source' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Sink' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Watermark and execution' })).toBeInTheDocument();
    fillRequiredFields();
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    await waitFor(() => expect(mockedJobsApi.createJob).toHaveBeenCalledTimes(1));
    const [request] = mockedJobsApi.createJob.mock.calls[0];
    expect(request).toMatchObject({
      name: 'New job',
      sourceConnect: 'jdbc:postgresql://source.example:5432/source_db',
      sourceTable: 'source_table',
      sinkConnect: 'jdbc:postgresql://sink.example:5432/sink_db',
      sinkTable: 'sink_table',
      mode: 'complete',
      jobs: 2,
      maxAttempts: 3,
      retryBackoffSeconds: 60,
      automaticRetryEnabled: false
    });
    expect(request.sourceUser).toBeUndefined();
    expect(request.sourcePassword).toBeUndefined();
    expect(request).not.toHaveProperty('incrementalWatermarkColumn');
    expect(request).not.toHaveProperty('initialWatermarkValue');
    expect(await screen.findByText('Saved job-new')).toBeInTheDocument();
  });

  it('prefills every editable field and disables the name in edit mode', async () => {
    mockedJobsApi.getJob.mockResolvedValue(baseJob);

    renderForm('/jobs/job-1/edit');

    expect(await screen.findByDisplayValue('Orders replication')).toBeDisabled();
    expect(screen.getByLabelText(/^Source connection/)).toHaveValue(baseJob.sourceConnect);
    expect(screen.getByLabelText(/^Source user/)).toHaveValue(baseJob.sourceUser);
    expect(screen.getByLabelText(/^Table/)).toHaveValue(baseJob.sourceTable);
    expect(screen.getByLabelText(/^Where/)).toHaveValue(baseJob.sourceWhere);
    expect(screen.getByLabelText(/^Sink connection/)).toHaveValue(baseJob.sinkConnect);
    expect(screen.getByLabelText(/^Sink user/)).toHaveValue(baseJob.sinkUser);
    expect(screen.getByLabelText(/^Sink table/)).toHaveValue(baseJob.sinkTable);
    expect(screen.getByRole('combobox', { name: 'Mode' })).toHaveTextContent(baseJob.mode ?? '');
    expect(screen.getByLabelText(/^Parallel tasks/)).toHaveValue(baseJob.jobs);
    expect(screen.getByLabelText(/^Maximum automatic attempts/)).toHaveValue(5);
    expect(screen.getByLabelText(/^Retry backoff/)).toHaveValue(90);
    expect(screen.getByLabelText('Automatic retry after lease expiry')).toBeChecked();
    expect(screen.getByLabelText(/^Incremental watermark column/)).toHaveValue(baseJob.incrementalWatermarkColumn);
    expect(screen.getByLabelText(/^Initial watermark value/)).toHaveValue(baseJob.initialWatermarkValue);
  });

  it('shows the keep-existing helper next to both edit password fields', async () => {
    mockedJobsApi.getJob.mockResolvedValue(baseJob);

    renderForm('/jobs/job-1/edit');

    await screen.findByDisplayValue('Orders replication');
    expect(screen.getAllByText('Leave blank to keep the existing value')).toHaveLength(2);
  });

  it('shows the complete-mode warning while editing an existing job', async () => {
    mockedJobsApi.getJob.mockResolvedValue({
      ...baseJob,
      mode: 'complete',
      modeWarning: 'Complete mode clears the sink before loading. If the run is interrupted or retried, the sink may be empty or partially populated. Use complete-atomic for an all-or-nothing load when supported.'
    });

    renderForm('/jobs/job-1/edit');

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Use complete-atomic for an all-or-nothing load when supported.'
    );
  });

  it('prefills recognized connection fields and keeps unknown parameters in the extra editor', async () => {
    mockedJobsApi.getJob.mockResolvedValue({
      ...baseJob,
      sourceConnect: 'jdbc:postgresql://source.example:5432/warehouse',
      sourceConnectionParams: { 'custom.option': 'enabled', format: 'RFC4180' },
      sinkConnect: 'kafka://broker.example:9092',
      sinkConnectionParams: { topic: 'orders', acks: 'all' }
    });

    renderForm('/jobs/job-1/edit');

    await screen.findByDisplayValue('Orders replication');
    expect(screen.getByRole('combobox', { name: 'Source data source type' })).toHaveTextContent('PostgreSQL');
    expect(screen.getByLabelText('Host')).toHaveValue('source.example');
    expect(screen.getByLabelText('Port')).toHaveValue(5432);
    expect(screen.getByLabelText('Database / SID or Service Name')).toHaveValue('warehouse');
    expect(screen.getAllByLabelText('Extra JDBC parameters')[0]).toHaveValue('custom.option=enabled');
    expect(screen.getByLabelText('Bootstrap servers')).toHaveValue('broker.example:9092');
    expect(screen.getByLabelText('Topic name')).toHaveValue('orders');
    expect(screen.getByRole('combobox', { name: 'ACKs' })).toHaveTextContent('all');
  });

  it('falls back to a custom connection string for an unknown scheme', async () => {
    mockedJobsApi.getJob.mockResolvedValue({
      ...baseJob,
      sourceConnect: 'jdbc:custom://source.example/database',
      sourceConnectionParams: {}
    });

    renderForm('/jobs/job-1/edit');

    await screen.findByDisplayValue('Orders replication');
    expect(screen.getByRole('combobox', { name: 'Source data source type' })).toHaveTextContent('Custom');
    expect(screen.getByDisplayValue('jdbc:custom://source.example/database')).toBeInTheDocument();
  });

  it.each(['complete', 'complete-atomic'] as const)(
    'disables watermarks and strips them from a %s update payload',
    async mode => {
      mockedJobsApi.getJob.mockResolvedValue(baseJob);
      mockedJobsApi.updateJob.mockResolvedValue({ id: 'job-1', name: baseJob.name });

      renderForm('/jobs/job-1/edit');
      await screen.findByDisplayValue('Orders replication');
      fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Mode' }));
      fireEvent.click(await screen.findByRole('option', { name: mode }));

      expect(screen.getByLabelText(/^Incremental watermark column/)).toBeDisabled();
      expect(screen.getByLabelText(/^Initial watermark value/)).toBeDisabled();

      fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
      await waitFor(() => expect(mockedJobsApi.updateJob).toHaveBeenCalledTimes(1));

      const [request] = mockedJobsApi.updateJob.mock.calls[0];
      expect(request).not.toHaveProperty('incrementalWatermarkColumn');
      expect(request).not.toHaveProperty('initialWatermarkValue');
    }
  );

  it('applies mode defaults and preserves an explicit retry choice', async () => {
    renderForm('/jobs/new');

    const automaticRetry = screen.getByLabelText('Automatic retry after lease expiry');
    expect(automaticRetry).not.toBeChecked();

    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Mode' }));
    fireEvent.click(await screen.findByRole('option', { name: 'incremental' }));
    expect(automaticRetry).toBeChecked();

    fireEvent.click(automaticRetry);
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Mode' }));
    fireEvent.click(await screen.findByRole('option', { name: 'complete' }));
    expect(automaticRetry).not.toBeChecked();
  });

  it('blocks submission when retry attempts or backoff are invalid', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    fillRequiredFields();
    fireEvent.change(screen.getByLabelText(/^Maximum automatic attempts/), { target: { value: '0' } });
    fireEvent.change(screen.getByLabelText(/^Retry backoff/), { target: { value: '-1' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByText('Maximum attempts must be at least 1.')).toBeInTheDocument();
    expect(screen.getByText('Retry backoff cannot be negative.')).toBeInTheDocument();
    expect(mockedJobsApi.createJob).not.toHaveBeenCalled();
  });

  it('renders a mutation ApiError and does not navigate', async () => {
    mockedJobsApi.createJob.mockRejectedValue(new ApiError(400, 'Invalid job', 'The source table is required.'));

    renderForm('/jobs/new');
    fillRequiredFields();
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('The source table is required.');
    expect(screen.getByRole('heading', { name: 'New job' })).toBeInTheDocument();
    expect(screen.queryByText(/Saved /)).not.toBeInTheDocument();
  });

  it('blocks submission and shows a field error when source table is blank', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    fillRequiredFields(false);
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByText('Source table or query is required.')).toBeInTheDocument();
    expect(mockedJobsApi.createJob).not.toHaveBeenCalled();
  });
});
