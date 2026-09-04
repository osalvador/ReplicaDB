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
  TableRow
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import { useState } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobs } from '../api/jobsApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

export default function JobsPage() {
  const navigate = useNavigate();
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
    return (
      <Alert severity="error" action={<Button color="inherit" size="small" onClick={() => void jobsQuery.refetch()}>Try again</Button>}>
        Unable to load jobs. Check the server connection and try again.
      </Alert>
    );
  }

  const jobs = jobsQuery.data.content ?? [];
  const totalElements = jobsQuery.data.totalElements ?? 0;
  const paginationCount = jobs.length < size ? page * size + jobs.length : totalElements;

  return (
    <>
      <PageHeader
        title="Jobs"
        description="Replication definitions available to your account."
        actions={<Button component={RouterLink} to="/jobs/new" variant="contained" startIcon={<AddIcon />}>New job</Button>}
      />
      <SurfaceSection title="Job catalog" description="Open a job to inspect its schedule, bindings, and run history.">
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
                <TableRow
                  key={job.id}
                  hover
                  tabIndex={0}
                  aria-label={`Open job ${job.name}`}
                  onClick={() => navigate(`/jobs/${job.id}`)}
                  onKeyDown={event => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault();
                      navigate(`/jobs/${job.id}`);
                    }
                  }}
                  sx={{ cursor: 'pointer', '&:focus-visible': { outline: theme => `2px solid ${theme.palette.primary.main}`, outlineOffset: -2 } }}
                >
                  <TableCell>
                    <Link component={RouterLink} to={`/jobs/${job.id}`} color="primary" underline="hover" sx={{ fontWeight: 700 }}>{job.name}</Link>
                  </TableCell>
                  <TableCell>{job.sourceTable}</TableCell>
                  <TableCell>{job.sinkTable}</TableCell>
                  <TableCell><Chip label={job.mode ?? 'Unknown'} variant="outlined" size="small" /></TableCell>
                </TableRow>
              ))}
              {jobs.length === 0 && <TableRow><TableCell colSpan={4}><EmptyState title="No jobs available." /></TableCell></TableRow>}
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
