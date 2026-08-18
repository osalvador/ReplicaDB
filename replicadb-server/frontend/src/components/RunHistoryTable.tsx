import {
  Alert,
  Chip,
  CircularProgress,
  Link,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Typography
} from '@mui/material';
import { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobRuns, type JobRunResponse, type JobRunStatus } from '../api/runsApi';

export const statusChipColors: Record<JobRunStatus, 'default' | 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning'> = {
  PENDING: 'info',
  RUNNING: 'primary',
  SUCCEEDED: 'success',
  FAILED: 'error',
  CANCEL_REQUESTED: 'warning',
  CANCELLED: 'default',
  RETRY_SCHEDULED: 'secondary'
};

function formatInstant(value: string | null | undefined): string {
  return value ?? 'Not started';
}

function RunRow({ run }: { run: JobRunResponse }) {
  const status = run.status;
  const color = status ? statusChipColors[status] : 'default';

  return (
    <TableRow hover>
      <TableCell>
        <Chip label={status ?? 'UNKNOWN'} color={color} size="small" />
      </TableCell>
      <TableCell>{run.attempt ?? '—'}</TableCell>
      <TableCell>{formatInstant(run.startedAt)}</TableCell>
      <TableCell>{formatInstant(run.finishedAt)}</TableCell>
      <TableCell align="right">
        <Link component={RouterLink} to={`/runs/${run.id}`} underline="hover">
          View run
        </Link>
      </TableCell>
    </TableRow>
  );
}

export default function RunHistoryTable({ jobId }: { jobId: string }) {
  const [page, setPage] = useState(0);
  const size = 50;
  const runsQuery = useQuery({
    queryKey: ['jobRuns', jobId, page, size],
    queryFn: () => listJobRuns(jobId, page, size),
    enabled: Boolean(jobId)
  });

  if (runsQuery.isPending) {
    return <CircularProgress aria-label="Loading run history" />;
  }

  if (runsQuery.isError || !runsQuery.data) {
    return <Alert severity="error">Unable to load run history.</Alert>;
  }

  const runs = runsQuery.data.content ?? [];
  const totalElements = runsQuery.data.totalElements ?? 0;
  const paginationCount = runs.length < size ? page * size + runs.length : totalElements;

  return (
    <Paper variant="outlined" elevation={0}>
      <Typography component="h2" variant="h5" sx={{ p: 3, pb: 0 }}>
        Run history
      </Typography>
      <TableContainer>
        <Table aria-label="Run history">
          <TableHead>
            <TableRow>
              <TableCell>Status</TableCell>
              <TableCell>Attempt</TableCell>
              <TableCell>Started</TableCell>
              <TableCell>Finished</TableCell>
              <TableCell align="right">Details</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {runs.map(run => <RunRow key={run.id} run={run} />)}
            {runs.length === 0 && (
              <TableRow>
                <TableCell colSpan={5}>
                  <Typography color="text.secondary">No runs recorded.</Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        component="div"
        count={paginationCount}
        page={page}
        rowsPerPage={size}
        rowsPerPageOptions={[size]}
        onPageChange={(_event, nextPage) => setPage(nextPage)}
      />
    </Paper>
  );
}
