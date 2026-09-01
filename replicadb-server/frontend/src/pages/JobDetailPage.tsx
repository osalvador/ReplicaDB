import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { Alert, Button, Stack, Typography } from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import { useAuth } from '../auth/useAuth';
import { getJob, updateJob, type JobDefinitionResponse } from '../api/jobsApi';
import type { components } from '../api/schema';
import LoadingState from '../components/LoadingState';
import JobScheduleCard from '../components/JobScheduleCard';
import PageHeader from '../components/PageHeader';
import RunHistoryTable from '../components/RunHistoryTable';
import SurfaceSection from '../components/SurfaceSection';
import { triggerRun } from '../api/runsApi';

function DefinitionRows({ details }: { details: Array<[string, string | number | undefined | null]> }) {
  return (
    <Stack divider={<div role="separator" />}>
      {details.map(([label, value]) => (
        <Stack key={label} direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ py: 1.25 }}>
          <Typography sx={{ minWidth: { sm: 210 } }} color="text.secondary">
            {label}
          </Typography>
          <Typography sx={{ overflowWrap: 'anywhere' }}>{value ?? 'Not configured'}</Typography>
        </Stack>
      ))}
    </Stack>
  );
}

type BindingSide = 'source' | 'sink';
type JobDefinitionRequest = components['schemas']['JobDefinitionRequest'];

function datasourceLabel(
  datasource: JobDefinitionResponse['sourceDatasource'],
  datasourceId: string | null | undefined
): string {
  if (datasource?.name) {
    return datasource.connectorType
      ? `${datasource.name} (${datasource.connectorType})`
      : datasource.name;
  }
  return datasourceId ? `Datasource ${datasourceId}` : 'Not configured';
}

function bindingRequest(job: JobDefinitionResponse, side: BindingSide, enabled: boolean): JobDefinitionRequest {
  return {
    name: job.name ?? '',
    sourceDatasourceId: job.sourceDatasourceId ?? '',
    sourceDatasourceUseEnabled: side === 'source' ? enabled : job.sourceDatasourceUseEnabled,
    sourceTable: job.sourceTable ?? undefined,
    sourceWhere: job.sourceWhere ?? undefined,
    sourceColumns: job.sourceColumns ?? undefined,
    sourceQuery: job.sourceQuery ?? undefined,
    sinkDatasourceId: job.sinkDatasourceId ?? '',
    sinkDatasourceUseEnabled: side === 'sink' ? enabled : job.sinkDatasourceUseEnabled,
    sinkTable: job.sinkTable ?? '',
    sinkColumns: job.sinkColumns ?? undefined,
    sinkStagingSchema: job.sinkStagingSchema ?? undefined,
    sinkStagingTable: job.sinkStagingTable ?? undefined,
    sinkDisableEscape: job.sinkDisableEscape,
    sinkDisableTruncate: job.sinkDisableTruncate,
    mode: job.mode ?? 'complete',
    jobs: job.jobs ?? 1,
    incrementalWatermarkColumn: job.incrementalWatermarkColumn ?? undefined,
    initialWatermarkValue: job.initialWatermarkValue ?? undefined,
    fetchSize: job.fetchSize,
    bandwidthThrottling: job.bandwidthThrottling,
    verbose: job.verbose,
    maxAttempts: job.maxAttempts,
    retryBackoffSeconds: job.retryBackoffSeconds,
    automaticRetryEnabled: job.automaticRetryEnabled
  };
}

export default function JobDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [triggerError, setTriggerError] = useState<string>();
  const [bindingError, setBindingError] = useState<string>();
  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: Boolean(id)
  });
  const triggerMutation = useMutation({
    mutationFn: () => triggerRun(id ?? ''),
    onSuccess: async result => {
      await queryClient.invalidateQueries({ queryKey: ['jobRuns', id ?? ''] });
      if (result.id) {
        navigate(`/runs/${result.id}`);
      }
    },
    onError: error => {
      setTriggerError(error instanceof ApiError ? error.detail : 'Unable to trigger a run.');
    }
  });
  const bindingMutation = useMutation({
    mutationFn: ({ side, enabled, job }: { side: BindingSide; enabled: boolean; job: JobDefinitionResponse }) =>
      updateJob(id ?? '', bindingRequest(job, side, enabled)),
    onSuccess: async () => {
      setBindingError(undefined);
      await queryClient.invalidateQueries({ queryKey: ['jobs', id] });
      await queryClient.invalidateQueries({ queryKey: ['jobs'] });
    },
    onError: error => {
      setBindingError(error instanceof ApiError ? error.detail : 'Unable to update datasource binding.');
    }
  });

  if (jobQuery.isPending) {
    return <LoadingState label="Loading job" />;
  }

  if (jobQuery.isError || !jobQuery.data) {
    return <Alert severity="error">Unable to load this job.</Alert>;
  }

  const job = jobQuery.data;
  const sourceDetails: Array<[string, string | number | undefined | null]> = [
    ['Datasource', datasourceLabel(job.sourceDatasource, job.sourceDatasourceId)],
    ['Binding', job.sourceDatasourceUseEnabled ? 'Enabled' : 'Disabled'],
    ['Source table', job.sourceTable],
    ['Source columns', job.sourceColumns],
    ['Source query', job.sourceQuery]
  ];
  const sinkDetails: Array<[string, string | number | undefined | null]> = [
    ['Datasource', datasourceLabel(job.sinkDatasource, job.sinkDatasourceId)],
    ['Binding', job.sinkDatasourceUseEnabled ? 'Enabled' : 'Disabled'],
    ['Sink table', job.sinkTable],
    ['Sink columns', job.sinkColumns],
    ['Staging schema', job.sinkStagingSchema],
    ['Staging table', job.sinkStagingTable],
    ['Escape values', job.sinkDisableEscape === undefined ? undefined : job.sinkDisableEscape ? 'Disabled' : 'Enabled'],
    ['Truncate sink table', job.sinkDisableTruncate === undefined ? undefined : job.sinkDisableTruncate ? 'Disabled' : 'Enabled']
  ];
  const executionDetails: Array<[string, string | number | undefined | null]> = [
    ['Mode', job.mode],
    ['Parallel tasks', job.jobs],
    ['Fetch size', job.fetchSize],
    ['Bandwidth throttling (KB/s)', job.bandwidthThrottling],
    ['Verbose', job.verbose === undefined ? undefined : job.verbose ? 'Enabled' : 'Disabled'],
    ['Watermark column', job.incrementalWatermarkColumn],
    ['Initial watermark', job.initialWatermarkValue]
  ];
  const lifecycleDetails: Array<[string, string | number | undefined | null]> = [
    ['Created', job.createdAt],
    ['Updated', job.updatedAt]
  ];

  return (
    <Stack spacing={3}>
      <PageHeader
        title={job.name}
        description="Read-only job definition"
        backLink={
          <Button component={RouterLink} to="/" variant="text" startIcon={<ArrowBackIcon />}>
            Back to jobs
          </Button>
        }
        actions={
          <>
            <Button
              variant="contained"
              onClick={() => {
                setTriggerError(undefined);
                triggerMutation.mutate();
              }}
              disabled={triggerMutation.isPending}
            >
              {triggerMutation.isPending ? 'Triggering...' : 'Trigger run'}
            </Button>
            <Button component={RouterLink} to={`/jobs/${id}/edit`} variant="outlined">
              Edit
            </Button>
            {user?.role === 'ADMIN' && (
              <Button component={RouterLink} to={`/jobs/${id}/permissions`} variant="outlined">
                Manage permissions
              </Button>
            )}
          </>
        }
      />
      {job.modeWarning && <Alert severity="warning">{job.modeWarning}</Alert>}
      {triggerError && <Alert severity="error">{triggerError}</Alert>}
      <Alert severity="info">
        Disabling either datasource binding blocks future manual and scheduled runs. It does not cancel active work.
      </Alert>
      {bindingError && <Alert severity="error">{bindingError}</Alert>}
      <Stack spacing={2}>
        <SurfaceSection
          title="Source"
          actions={
            <Button
              size="small"
              variant="outlined"
              onClick={() => bindingMutation.mutate({
                side: 'source',
                enabled: !job.sourceDatasourceUseEnabled,
                job
              })}
              disabled={bindingMutation.isPending}
            >
              {job.sourceDatasourceUseEnabled ? 'Disable binding' : 'Enable binding'}
            </Button>
          }
        >
          <DefinitionRows details={sourceDetails} />
        </SurfaceSection>
        <SurfaceSection
          title="Sink"
          actions={
            <Button
              size="small"
              variant="outlined"
              onClick={() => bindingMutation.mutate({
                side: 'sink',
                enabled: !job.sinkDatasourceUseEnabled,
                job
              })}
              disabled={bindingMutation.isPending}
            >
              {job.sinkDatasourceUseEnabled ? 'Disable binding' : 'Enable binding'}
            </Button>
          }
        >
          <DefinitionRows details={sinkDetails} />
        </SurfaceSection>
        <SurfaceSection title="Execution">
          <DefinitionRows details={executionDetails} />
        </SurfaceSection>
        <SurfaceSection title="Lifecycle">
          <DefinitionRows details={lifecycleDetails} />
        </SurfaceSection>
      </Stack>
      <JobScheduleCard jobId={id ?? ''} />
      <RunHistoryTable jobId={id ?? ''} />
    </Stack>
  );
}
