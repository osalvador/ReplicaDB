import {
  Alert,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow
} from '@mui/material';
import { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobRuns, type JobRunResponse } from '../api/runsApi';
import EmptyState from './EmptyState';
import LoadingState from './LoadingState';
import StatusChip, { statusChipColors } from './StatusChip';
import SurfaceSection from './SurfaceSection';

export { statusChipColors } from './StatusChip';

function formatInstant(value: string | null | undefined): string {
  return value ?? 'Not started';
}

function RunRow({ run }: { run: JobRunResponse }) {
  return (
    <TableRow hover>
      <TableCell>
        <StatusChip status={run.status} />
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
    return <LoadingState label="Loading run history" />;
  }

  if (runsQuery.isError || !runsQuery.data) {
    return <Alert severity="error">Unable to load run history.</Alert>;
  }

  const runs = runsQuery.data.content ?? [];
  const totalElements = runsQuery.data.totalElements ?? 0;
  const paginationCount = runs.length < size ? page * size + runs.length : totalElements;

  return (
    <SurfaceSection title="Run history">
      <TableContainer sx={{ overflowX: 'auto' }}>
        <Table aria-label="Run history" sx={{ minWidth: 640 }}>
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
                  <EmptyState title="No runs recorded." />
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
    </SurfaceSection>
  );
}
