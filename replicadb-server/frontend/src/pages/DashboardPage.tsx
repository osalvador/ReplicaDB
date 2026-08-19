import {
  Alert,
  Button,
  Chip,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
} from '@mui/material';
import { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobs } from '../api/jobsApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

export default function DashboardPage() {
  const [page, setPage] = useState(0);
  const size = 50;
  const jobsQuery = useQuery({
    queryKey: ['jobs', page, size],
    queryFn: () => listJobs(page, size)
  });

  if (jobsQuery.isPending) {
    return <LoadingState label="Loading jobs" />;
  }

  if (jobsQuery.isError) {
    return <Alert severity="error">Unable to load jobs.</Alert>;
  }

  const jobs = jobsQuery.data.content ?? [];
  const totalElements = jobsQuery.data.totalElements ?? 0;
  const paginationCount = jobs.length < size ? page * size + jobs.length : totalElements;

  return (
    <>
      <PageHeader
        title="Dashboard"
        description="Jobs available to your account"
        actions={
          <Button component={RouterLink} to="/jobs/new" variant="contained">
            New job
          </Button>
        }
      />
      <SurfaceSection title="Jobs">
        <TableContainer sx={{ overflowX: 'auto' }}>
          <Table aria-label="Jobs" sx={{ minWidth: 640 }}>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Source</TableCell>
                <TableCell>Sink</TableCell>
                <TableCell>Mode</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {jobs.map(job => (
                <TableRow key={job.id} hover>
                  <TableCell>
                    <Link component={RouterLink} to={`/jobs/${job.id}`} color="primary" underline="hover" sx={{ fontWeight: 700 }}>
                      {job.name}
                    </Link>
                  </TableCell>
                  <TableCell>{job.sourceTable}</TableCell>
                  <TableCell>{job.sinkTable}</TableCell>
                  <TableCell>
                    <Chip label={job.mode ?? 'Unknown'} variant="outlined" size="small" />
                  </TableCell>
                </TableRow>
              ))}
              {jobs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4}>
                    <EmptyState title="No jobs available." />
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
    </>
  );
}
