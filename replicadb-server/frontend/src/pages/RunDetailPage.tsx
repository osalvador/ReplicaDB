import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { Alert, Button, Stack, Typography } from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import { getRun, getRunLog } from '../api/runsApi';
import { cancelRun, retryRun } from '../api/runsApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusChip from '../components/StatusChip';
import SurfaceSection from '../components/SurfaceSection';
import { getRunRefetchInterval } from '../utils/runStatus';

function displayValue(value: string | number | null | undefined): string | number {
  return value ?? 'Not available';
}

function MetricRows({ details }: { details: Array<[string, string | number | null | undefined]> }) {
  return (
    <Stack divider={<div role="separator" />}>
      {details.map(([label, value]) => (
        <Stack key={label} direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ py: 1.25 }}>
          <Typography color="text.secondary" sx={{ minWidth: { sm: 190 } }}>
            {label}
          </Typography>
          <Typography sx={{ overflowWrap: 'anywhere' }}>{displayValue(value)}</Typography>
        </Stack>
      ))}
    </Stack>
  );
}

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [actionError, setActionError] = useState<string>();
  const [cancelWarning, setCancelWarning] = useState<string>();
  const runQuery = useQuery({
    queryKey: ['runs', id],
    queryFn: () => getRun(id ?? ''),
    enabled: Boolean(id),
    refetchInterval: query => getRunRefetchInterval(query.state.data?.status)
  });
  const logQuery = useQuery({
    queryKey: ['runLog', id],
    queryFn: () => getRunLog(id ?? ''),
    enabled: Boolean(id)
  });
  const cancelMutation = useMutation({
    mutationFn: () => cancelRun(id ?? ''),
    onSuccess: response => {
      setCancelWarning(response.warning);
      void queryClient.invalidateQueries({ queryKey: ['runs', id ?? ''] });
    },
    onError: error => {
      setActionError(error instanceof ApiError ? error.detail : 'Unable to cancel this run.');
    }
  });
  const retryMutation = useMutation({
    mutationFn: () => retryRun(id ?? ''),
    onSuccess: result => {
      if (result.id) {
        navigate(`/runs/${result.id}`);
      }
    },
    onError: error => {
      setActionError(error instanceof ApiError ? error.detail : 'Unable to retry this run.');
    }
  });

  if (runQuery.isPending) {
    return <LoadingState label="Loading run" />;
  }

  if (runQuery.isError || !runQuery.data) {
    return <Alert severity="error">Unable to load this run.</Alert>;
  }

  const run = runQuery.data;
  const details: Array<[string, string | number | null | undefined]> = [
    ['Attempt', run.attempt],
    ['Rows processed', run.rowsProcessed],
    ['Duration', run.durationMillis == null ? null : `${run.durationMillis} ms`],
    ['Committed watermark', run.committedWatermark],
    ['Available', run.availableAt],
    ['Started', run.startedAt],
    ['Finished', run.finishedAt]
  ];

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Run detail"
        description={run.id}
        backLink={
          <Button component={RouterLink} to={`/jobs/${run.jobDefinitionId}`} variant="text" startIcon={<ArrowBackIcon />}>
            Back to job
          </Button>
        }
        actions={
          <>
          {(run.status === 'PENDING' || run.status === 'RUNNING' || run.status === 'CANCEL_REQUESTED') && (
            <Button
              variant="contained"
              color="warning"
              onClick={() => {
                setActionError(undefined);
                cancelMutation.mutate();
              }}
              disabled={cancelMutation.isPending}
            >
              {cancelMutation.isPending ? 'Cancelling...' : 'Cancel run'}
            </Button>
          )}
          {run.status === 'FAILED' && (
            <Button
              variant="contained"
              onClick={() => {
                setActionError(undefined);
                retryMutation.mutate();
              }}
              disabled={retryMutation.isPending}
            >
              {retryMutation.isPending ? 'Retrying...' : 'Retry run'}
            </Button>
          )}
          </>
        }
      />
      <SurfaceSection title="Run metrics">
        <Stack spacing={1}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'flex-start', sm: 'center' }} sx={{ pb: 1.25 }}>
            <Typography color="text.secondary" sx={{ minWidth: { sm: 190 } }}>Status</Typography>
            <StatusChip status={run.status} />
          </Stack>
          <MetricRows details={details} />
        </Stack>
      </SurfaceSection>
      {run.cancellationWarning && <Alert severity="warning">{run.cancellationWarning}</Alert>}
      {cancelWarning && <Alert severity="warning">{cancelWarning}</Alert>}
      {actionError && <Alert severity="error">{actionError}</Alert>}
      {run.errorMessage && <Alert severity="error">{run.errorMessage}</Alert>}
      <SurfaceSection title="Log excerpt">
        {logQuery.isPending ? (
          <LoadingState label="Loading log excerpt" compact />
        ) : logQuery.isError ? (
          <Alert severity="error">Unable to load the run log.</Alert>
        ) : logQuery.data?.excerpt ? (
          <Typography
            component="pre"
            sx={{
              maxWidth: '100%',
              overflowX: 'auto',
              whiteSpace: 'pre-wrap',
              overflowWrap: 'anywhere',
              fontFamily: 'monospace',
              m: 0
            }}
          >
            {logQuery.data.excerpt}
          </Typography>
        ) : (
          <EmptyState title="No log excerpt available." />
        )}
      </SurfaceSection>
    </Stack>
  );
}
