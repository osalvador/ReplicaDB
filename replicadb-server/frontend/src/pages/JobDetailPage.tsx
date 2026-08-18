import { Alert, CircularProgress, Divider, Paper, Stack, Typography } from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { useParams } from 'react-router-dom';
import { getJob } from '../api/jobsApi';
import RunHistoryTable from '../components/RunHistoryTable';

export default function JobDetailPage() {
  const { id } = useParams<{ id: string }>();
  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: Boolean(id)
  });

  if (jobQuery.isPending) {
    return <CircularProgress aria-label="Loading job" />;
  }

  if (jobQuery.isError || !jobQuery.data) {
    return <Alert severity="error">Unable to load this job.</Alert>;
  }

  const job = jobQuery.data;
  const details = [
    ['Source table', job.sourceTable],
    ['Sink table', job.sinkTable],
    ['Mode', job.mode],
    ['Parallel tasks', job.jobs],
    ['Watermark column', job.incrementalWatermarkColumn],
    ['Initial watermark', job.initialWatermarkValue],
    ['Created', job.createdAt],
    ['Updated', job.updatedAt]
  ];

  return (
    <Stack spacing={3}>
      <div>
        <Typography component="h1" variant="h3">
          {job.name}
        </Typography>
        <Typography color="text.secondary">Read-only job definition</Typography>
      </div>
      {job.modeWarning && <Alert severity="warning">{job.modeWarning}</Alert>}
      <Paper variant="outlined" elevation={0} sx={{ p: 3 }}>
        <Stack divider={<Divider flexItem />}>
          {details.map(([label, value]) => (
            <Stack key={label} direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ py: 1.5 }}>
              <Typography sx={{ minWidth: 190 }} color="text.secondary">
                {label}
              </Typography>
              <Typography>{value ?? 'Not configured'}</Typography>
            </Stack>
          ))}
        </Stack>
      </Paper>
      <RunHistoryTable jobId={id ?? ''} />
    </Stack>
  );
}
