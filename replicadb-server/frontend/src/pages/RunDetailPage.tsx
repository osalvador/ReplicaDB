import { Alert, Chip, CircularProgress, Divider, Paper, Stack, Typography } from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { useParams } from 'react-router-dom';
import { getRun, getRunLog } from '../api/runsApi';
import { statusChipColors } from '../components/RunHistoryTable';
import { getRunRefetchInterval } from '../utils/runStatus';

function displayValue(value: string | number | null | undefined): string | number {
  return value ?? 'Not available';
}

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
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

  if (runQuery.isPending) {
    return <CircularProgress aria-label="Loading run" />;
  }

  if (runQuery.isError || !runQuery.data) {
    return <Alert severity="error">Unable to load this run.</Alert>;
  }

  const run = runQuery.data;
  const statusColor = run.status ? statusChipColors[run.status] : 'default';
  const details = [
    ['Attempt', run.attempt],
    ['Rows processed', run.rowsProcessed],
    ['Duration', run.durationMillis == null ? null : `${run.durationMillis} ms`],
    ['Committed watermark', run.committedWatermark],
    ['Started', run.startedAt],
    ['Finished', run.finishedAt]
  ];

  return (
    <Stack spacing={3}>
      <div>
        <Typography component="h1" variant="h3">
          Run detail
        </Typography>
        <Typography color="text.secondary">{run.id}</Typography>
      </div>
      <Paper variant="outlined" elevation={0} sx={{ p: 3 }}>
        <Stack spacing={2} divider={<Divider flexItem />}>
          <Stack direction="row" justifyContent="space-between" alignItems="center">
            <Typography color="text.secondary">Status</Typography>
            <Chip label={run.status ?? 'UNKNOWN'} color={statusColor} />
          </Stack>
          {details.map(([label, value]) => (
            <Stack key={label} direction={{ xs: 'column', sm: 'row' }} spacing={2}>
              <Typography color="text.secondary" sx={{ minWidth: 190 }}>{label}</Typography>
              <Typography>{displayValue(value)}</Typography>
            </Stack>
          ))}
        </Stack>
      </Paper>
      {run.cancellationWarning && <Alert severity="warning">{run.cancellationWarning}</Alert>}
      {run.errorMessage && <Alert severity="error">{run.errorMessage}</Alert>}
      <Paper variant="outlined" elevation={0} sx={{ p: 3 }}>
        <Typography component="h2" variant="h5" gutterBottom>
          Log excerpt
        </Typography>
        <Typography component="pre" sx={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', m: 0 }}>
          {logQuery.data?.excerpt ?? 'No log excerpt available.'}
        </Typography>
      </Paper>
    </Stack>
  );
}
