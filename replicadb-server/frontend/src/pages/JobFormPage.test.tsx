import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useParams } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as datasourcesApi from '../api/datasourcesApi';
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

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return { ...actual, listDatasources: vi.fn() };
});

const mockedJobsApi = vi.mocked(jobsApi);
const mockedDatasourcesApi = vi.mocked(datasourcesApi);

const sourceDatasource = {
  id: 'source-1',
  name: 'Source database',
  connectorType: 'postgres',
  safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/source',
  canUse: true
};

const sinkDatasource = {
  id: 'sink-1',
  name: 'Sink database',
  connectorType: 'postgres',
  safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/sink',
  canUse: true
};

const baseJob: JobDefinitionResponse = {
  id: 'job-1',
  name: 'Orders replication',
  sourceDatasourceId: 'source-1',
  sourceDatasource: sourceDatasource,
  sourceDatasourceUseEnabled: true,
  sourceTable: 'orders',
  sourceWhere: 'region = north',
  sinkDatasourceId: 'sink-1',
  sinkDatasource: sinkDatasource,
  sinkDatasourceUseEnabled: true,
  sinkTable: 'warehouse_orders',
  mode: 'incremental',
  jobs: 4,
  incrementalWatermarkColumn: 'updated_at',
  initialWatermarkValue: '0',
  createdAt: '2026-08-18T10:00:00Z',
  updatedAt: '2026-08-18T11:00:00Z',
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

async function selectDatasource(label: string, name: string) {
  const input = await screen.findByRole('combobox', { name });
  fireEvent.mouseDown(input);
  fireEvent.change(input, { target: { value: label } });
  fireEvent.click(await screen.findByRole('option', { name: new RegExp(label) }));
}

async function fillRequiredFields(includeSourceTable = true) {
  fireEvent.change(screen.getByLabelText(/^Name/), { target: { value: 'New job' } });
  await selectDatasource('Source', 'Source datasource');
  await selectDatasource('Sink', 'Sink datasource');
  if (includeSourceTable) {
    fireEvent.change(screen.getByLabelText(/^Table/), { target: { value: 'source_table' } });
  }
  fireEvent.change(screen.getByLabelText(/^Sink table/), { target: { value: 'sink_table' } });
  fireEvent.change(screen.getByLabelText(/^Parallel tasks/), { target: { value: '2' } });
}

describe('JobFormPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedDatasourcesApi.listDatasources.mockImplementation(async (_page, _size, role) => ({
      content: role === 'source' ? [sourceDatasource] : [sinkDatasource],
      page: 0,
      size: 200,
      totalElements: 1
    }));
  });

  it('creates a job with a normalized payload and navigates to the returned job', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    expect(screen.getByRole('heading', { level: 2, name: 'Basics' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Source' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Sink' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Watermark and execution' })).toBeInTheDocument();
    await fillRequiredFields();
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    await waitFor(() => expect(mockedJobsApi.createJob).toHaveBeenCalledTimes(1));
    const [request] = mockedJobsApi.createJob.mock.calls[0];
    expect(request).toMatchObject({
      name: 'New job',
      sourceDatasourceId: 'source-1',
      sourceDatasourceUseEnabled: true,
      sourceTable: 'source_table',
      sinkDatasourceId: 'sink-1',
      sinkDatasourceUseEnabled: true,
      sinkTable: 'sink_table',
      mode: 'complete',
      jobs: 2,
      maxAttempts: 3,
      retryBackoffSeconds: 60,
      automaticRetryEnabled: false
    });
    expect(request).not.toHaveProperty('sourceConnect');
    expect(request).not.toHaveProperty('sinkConnect');
    expect(request).not.toHaveProperty('sourcePassword');
    expect(request).not.toHaveProperty('sinkPassword');
    expect(request).not.toHaveProperty('incrementalWatermarkColumn');
    expect(request).not.toHaveProperty('initialWatermarkValue');
    expect(await screen.findByText('Saved job-new')).toBeInTheDocument();
  });

  it('prefills every editable field and disables the name in edit mode', async () => {
    mockedJobsApi.getJob.mockResolvedValue(baseJob);

    renderForm('/jobs/job-1/edit');

    expect(await screen.findByDisplayValue('Orders replication')).toBeDisabled();
    expect(screen.getByRole('combobox', { name: 'Source datasource' })).toHaveValue('Source database (postgres)');
    expect(screen.getByRole('checkbox', { name: 'Source binding enabled' })).toBeChecked();
    expect(screen.getByLabelText(/^Table/)).toHaveValue(baseJob.sourceTable);
    expect(screen.getByLabelText(/^Where/)).toHaveValue(baseJob.sourceWhere);
    expect(screen.getByRole('combobox', { name: 'Sink datasource' })).toHaveValue('Sink database (postgres)');
    expect(screen.getByRole('checkbox', { name: 'Sink binding enabled' })).toBeChecked();
    expect(screen.getByLabelText(/^Sink table/)).toHaveValue(baseJob.sinkTable);
    expect(screen.getByRole('combobox', { name: 'Mode' })).toHaveTextContent(baseJob.mode ?? '');
    expect(screen.getByLabelText(/^Parallel tasks/)).toHaveValue(baseJob.jobs);
    expect(screen.getByLabelText(/^Maximum automatic attempts/)).toHaveValue(5);
    expect(screen.getByLabelText(/^Retry backoff/)).toHaveValue(90);
    expect(screen.getByLabelText('Automatic retry after lease expiry')).toBeChecked();
    expect(screen.getByLabelText(/^Incremental watermark column/)).toHaveValue(baseJob.incrementalWatermarkColumn);
    expect(screen.getByLabelText(/^Initial watermark value/)).toHaveValue(baseJob.initialWatermarkValue);
  });

  it('does not render inline credential or connection parameter controls', async () => {
    mockedJobsApi.getJob.mockResolvedValue(baseJob);

    renderForm('/jobs/job-1/edit');

    await screen.findByDisplayValue('Orders replication');
    expect(screen.queryByLabelText(/^Source connection/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Sink connection/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Source user/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Sink user/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Source password/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Sink password/)).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Extra JDBC parameters')).not.toBeInTheDocument();
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

  it('loads source and sink datasource options from role-filtered USE queries', async () => {
    renderForm('/jobs/new');

    expect(await screen.findByRole('combobox', { name: 'Source datasource' })).toBeInTheDocument();
    expect(await screen.findByRole('combobox', { name: 'Sink datasource' })).toBeInTheDocument();
    expect(mockedDatasourcesApi.listDatasources).toHaveBeenCalledWith(0, 200, 'source');
    expect(mockedDatasourcesApi.listDatasources).toHaveBeenCalledWith(0, 200, 'sink');

    await selectDatasource('Source', 'Source datasource');
    expect(screen.getByRole('combobox', { name: 'Source datasource' })).toHaveValue('Source database (postgres)');
    expect(screen.queryByLabelText(/^Source password/)).not.toBeInTheDocument();
  });

  it('keeps a disabled current binding visible without making unavailable datasources bindable', async () => {
    mockedJobsApi.getJob.mockResolvedValue({
      ...baseJob,
      sourceDatasourceUseEnabled: false,
      sourceDatasource: { ...sourceDatasource, id: 'source-1' }
    });
    mockedDatasourcesApi.listDatasources.mockImplementation(async (_page, _size, role) => ({
      content: role === 'source' ? [{ ...sourceDatasource, canUse: false }] : [sinkDatasource],
      page: 0,
      size: 200,
      totalElements: 1
    }));

    renderForm('/jobs/job-1/edit');

    expect(await screen.findByDisplayValue('Orders replication')).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Source datasource' })).toHaveValue('Source database (postgres)');
    expect(screen.getByRole('checkbox', { name: 'Source binding enabled' })).not.toBeChecked();
    expect(screen.getByText(/cannot be re-enabled until you have USE access/)).toBeInTheDocument();
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

  it('requires a watermark column before submitting incremental mode', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });
    renderForm('/jobs/new');
    await fillRequiredFields();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Mode' }));
    fireEvent.click(await screen.findByRole('option', { name: 'incremental' }));

    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByText('Watermark column is required for incremental mode.')).toBeInTheDocument();
    expect(mockedJobsApi.createJob).not.toHaveBeenCalled();
  });

  it('blocks submission when retry attempts or backoff are invalid', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    await fillRequiredFields();
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
    await fillRequiredFields();
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('The source table is required.');
    expect(screen.getByRole('heading', { name: 'New job' })).toBeInTheDocument();
    expect(screen.queryByText(/Saved /)).not.toBeInTheDocument();
  });

  it('blocks submission and shows a field error when source table is blank', async () => {
    mockedJobsApi.createJob.mockResolvedValue({ id: 'job-new', name: 'New job' });

    renderForm('/jobs/new');
    await fillRequiredFields(false);
    fireEvent.click(screen.getByRole('button', { name: 'Create job' }));

    expect(await screen.findByText('Source table or query is required.')).toBeInTheDocument();
    expect(mockedJobsApi.createJob).not.toHaveBeenCalled();
  });
});
