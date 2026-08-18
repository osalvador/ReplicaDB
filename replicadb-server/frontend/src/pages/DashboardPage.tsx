import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import {
  Alert,
  CircularProgress,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Tooltip,
  Typography
} from '@mui/material';
import { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobs } from '../api/jobsApi';

export default function DashboardPage() {
  const [page, setPage] = useState(0);
  const size = 50;
  const jobsQuery = useQuery({
    queryKey: ['jobs', page, size],
    queryFn: () => listJobs(page, size)
  });

  if (jobsQuery.isPending) {
    return <CircularProgress aria-label="Loading jobs" />;
  }

  if (jobsQuery.isError) {
    return <Alert severity="error">Unable to load jobs.</Alert>;
  }

  const jobs = jobsQuery.data.content ?? [];
  const totalElements = jobsQuery.data.totalElements ?? 0;
  const paginationCount = jobs.length < size ? page * size + jobs.length : totalElements;

  return (
    <>
      <Typography component="h1" variant="h3" gutterBottom>
        Dashboard
      </Typography>
      <Typography color="text.secondary" sx={{ mb: 3 }}>
        Jobs available to your account
      </Typography>
      <Paper elevation={0} variant="outlined">
        <TableContainer>
          <Table aria-label="Jobs">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Source</TableCell>
                <TableCell>Sink</TableCell>
                <TableCell>Mode</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {jobs.map(job => (
                <TableRow key={job.id} hover>
                  <TableCell>
                    <Typography component={RouterLink} to={`/jobs/${job.id}`} color="primary" sx={{ fontWeight: 700 }}>
                      {job.name}
                    </Typography>
                  </TableCell>
                  <TableCell>{job.sourceTable}</TableCell>
                  <TableCell>{job.sinkTable}</TableCell>
                  <TableCell>{job.mode}</TableCell>
                  <TableCell align="right">
                    {job.modeWarning && (
                      <Tooltip title={job.modeWarning}>
                        <IconButton aria-label={`Job warning: ${job.modeWarning}`} size="small" color="warning">
                          <WarningAmberIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {jobs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5}>
                    <Typography color="text.secondary">No jobs available.</Typography>
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
    </>
  );
}
