import {
  Alert,
  Button,
  Chip,
  Link,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  Stack,
  TextField
} from '@mui/material';
import ClearIcon from '@mui/icons-material/Clear';
import CloseIcon from '@mui/icons-material/Close';
import { useState, type KeyboardEvent, type MouseEvent } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listJobRuns, type JobRunFilters, type JobRunResponse, type JobRunStatus } from '../api/runsApi';
import EmptyState from './EmptyState';
import LoadingState from './LoadingState';
import StatusChip, { statusChipColors } from './StatusChip';
import SurfaceSection from './SurfaceSection';

export { statusChipColors } from './StatusChip';

function formatInstant(value: string | null | undefined): string {
  return value ?? 'Not started';
}

function formatDuration(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return '—';
  }

  const totalSeconds = Math.round(value / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  return minutes === 0 ? `${seconds}s` : `${minutes}m ${seconds}s`;
}

type SortKey = 'status' | 'attempt' | 'availableAt' | 'startedAt' | 'finishedAt' | 'durationMillis';
type SortDirection = 'asc' | 'desc';
type SortState = { key: SortKey; direction: SortDirection };
type RunHistoryFilterState = { status: JobRunStatus[]; from: string; to: string };

const runStatuses: JobRunStatus[] = [
  'PENDING',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'CANCEL_REQUESTED',
  'CANCELLED',
  'RETRY_SCHEDULED'
];

const emptyFilters: RunHistoryFilterState = { status: [], from: '', to: '' };

function toFilterInstant(value: string, endExclusive = false): string | undefined {
  if (!value) {
    return undefined;
  }

  const date = new Date(`${value}T00:00:00.000Z`);
  if (endExclusive) {
    date.setUTCDate(date.getUTCDate() + 1);
  }
  return date.toISOString();
}

function FilterBar({
  filters,
  onStatusChange,
  onFromChange,
  onToChange,
  onReset
}: {
  filters: RunHistoryFilterState;
  onStatusChange: (value: JobRunStatus[]) => void;
  onFromChange: (value: string) => void;
  onToChange: (value: string) => void;
  onReset: () => void;
}) {
  const hasFilters = Boolean(filters.status.length || filters.from || filters.to);

  return (
    <Stack
      direction={{ xs: 'column', sm: 'row' }}
      spacing={1}
      alignItems={{ xs: 'stretch', sm: 'center' }}
      sx={{ flexWrap: { sm: 'wrap' } }}
    >
      <TextField
        select
        label="Status"
        value={filters.status}
        onChange={event => {
          const value = event.target.value;
          onStatusChange(typeof value === 'string' ? value.split(',') as JobRunStatus[] : value as JobRunStatus[]);
        }}
        SelectProps={{
          multiple: true,
          displayEmpty: true,
          renderValue: selected => {
            const statuses = selected as JobRunStatus[];
            return statuses.length === 0 ? 'All statuses' : (
              <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', gap: 0.5 }}>
                {statuses.map(status => (
                  <Chip
                    key={status}
                    label={status}
                    size="small"
                    deleteIcon={<CloseIcon aria-label={`Remove ${status}`} />}
                    onDelete={() => onStatusChange(statuses.filter(selectedStatus => selectedStatus !== status))}
                    onMouseDown={event => event.stopPropagation()}
                  />
                ))}
              </Stack>
            );
          }
        }}
        InputLabelProps={{ shrink: true }}
        sx={{ minWidth: { sm: 260 } }}
      >
        {runStatuses.map(status => <MenuItem key={status} value={status}>{status}</MenuItem>)}
      </TextField>
      <TextField
        label="From date"
        type="date"
        value={filters.from}
        onChange={event => onFromChange(event.target.value)}
        InputLabelProps={{ shrink: true }}
      />
      <TextField
        label="To date"
        type="date"
        value={filters.to}
        onChange={event => onToChange(event.target.value)}
        InputLabelProps={{ shrink: true }}
      />
      <Button
        variant="text"
        startIcon={<ClearIcon />}
        onClick={onReset}
        disabled={!hasFilters}
      >
        Clear filters
      </Button>
    </Stack>
  );
}

function sortableValue(run: JobRunResponse, key: SortKey): string | number | null {
  if (key === 'status') {
    return run.status ?? null;
  }
  if (key === 'attempt') {
    return run.attempt ?? null;
  }
  if (key === 'durationMillis') {
    return run.durationMillis ?? null;
  }

  const value = run[key];
  return value ? Date.parse(value) : null;
}

function sortRuns(runs: JobRunResponse[], sort: SortState): JobRunResponse[] {
  return runs
    .map((run, index) => ({ run, index }))
    .sort((left, right) => {
      const leftValue = sortableValue(left.run, sort.key);
      const rightValue = sortableValue(right.run, sort.key);

      if (leftValue === null && rightValue === null) {
        return left.index - right.index;
      }
      if (leftValue === null) {
        return 1;
      }
      if (rightValue === null) {
        return -1;
      }

      const comparison = typeof leftValue === 'string' && typeof rightValue === 'string'
        ? leftValue.localeCompare(rightValue)
        : Number(leftValue) - Number(rightValue);

      return comparison === 0
        ? left.index - right.index
        : comparison * (sort.direction === 'asc' ? 1 : -1);
    })
    .map(({ run }) => run);
}

function SortableHeader({
  label,
  sortKey,
  sort,
  onSort
}: {
  label: string;
  sortKey: SortKey;
  sort: SortState;
  onSort: (key: SortKey) => void;
}) {
  const active = sort.key === sortKey;

  return (
    <TableCell sortDirection={active ? sort.direction : false}>
      <TableSortLabel
        active={active}
        direction={active ? sort.direction : 'asc'}
        onClick={() => onSort(sortKey)}
        aria-label={`Sort by ${label}`}
      >
        {label}
      </TableSortLabel>
    </TableCell>
  );
}

function RunRow({ run }: { run: JobRunResponse }) {
  const navigate = useNavigate();
  const runPath = `/runs/${run.id}`;
  const openRun = () => navigate(runPath);
  const handleClick = (event: MouseEvent<HTMLTableRowElement>) => {
    if (event.target instanceof Element && event.target.closest('a,button')) {
      return;
    }
    openRun();
  };
  const handleKeyDown = (event: KeyboardEvent<HTMLTableRowElement>) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openRun();
    }
  };

  return (
    <TableRow
      hover
      tabIndex={0}
      role="link"
      aria-label={`View run ${run.id}`}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      sx={{ cursor: 'pointer' }}
    >
      <TableCell>
        <StatusChip status={run.status} />
      </TableCell>
      <TableCell>{run.attempt ?? '—'}</TableCell>
      <TableCell>{formatInstant(run.availableAt)}</TableCell>
      <TableCell>{formatInstant(run.startedAt)}</TableCell>
      <TableCell>{formatInstant(run.finishedAt)}</TableCell>
      <TableCell>{formatDuration(run.durationMillis)}</TableCell>
      <TableCell align="right">
        <Link component={RouterLink} to={runPath} underline="hover">
          View run
        </Link>
      </TableCell>
    </TableRow>
  );
}

export default function RunHistoryTable({ jobId }: { jobId: string }) {
  const [page, setPage] = useState(0);
  const [filters, setFilters] = useState<RunHistoryFilterState>(emptyFilters);
  const [sort, setSort] = useState<SortState>({ key: 'startedAt', direction: 'desc' });
  const size = 50;
  const queryFilters: JobRunFilters = {
    status: filters.status.length ? filters.status : undefined,
    from: toFilterInstant(filters.from),
    to: toFilterInstant(filters.to, true)
  };
  const dateRangeInvalid = Boolean(filters.from && filters.to && filters.from > filters.to);
  const runsQuery = useQuery({
    queryKey: ['jobRuns', jobId, page, size, queryFilters],
    queryFn: () => listJobRuns(jobId, page, size, queryFilters),
    enabled: Boolean(jobId) && !dateRangeInvalid,
    placeholderData: previousData => previousData
  });

  const updateFilters = (update: Partial<RunHistoryFilterState>) => {
    setFilters(current => ({ ...current, ...update }));
    setPage(0);
  };
  const filterBar = (
    <FilterBar
      filters={filters}
      onStatusChange={status => updateFilters({ status })}
      onFromChange={from => updateFilters({ from })}
      onToChange={to => updateFilters({ to })}
      onReset={() => updateFilters(emptyFilters)}
    />
  );

  if (dateRangeInvalid) {
    return (
      <SurfaceSection title="Run history" actions={filterBar}>
        <Alert severity="warning">The start date must be on or before the end date.</Alert>
      </SurfaceSection>
    );
  }

  if (runsQuery.isPending) {
    return <LoadingState label="Loading run history" />;
  }

  if (runsQuery.isError || !runsQuery.data) {
    return <Alert severity="error">Unable to load run history.</Alert>;
  }

  const runs = runsQuery.data.content ?? [];
  const sortedRuns = sortRuns(runs, sort);
  const totalElements = runsQuery.data.totalElements ?? 0;
  const paginationCount = runs.length < size ? page * size + runs.length : totalElements;
  const handleSort = (key: SortKey) => {
    setSort(current => current.key === key
      ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
      : { key, direction: key === 'status' ? 'asc' : 'desc' });
  };

  return (
    <SurfaceSection title="Run history" actions={filterBar}>
      <TableContainer sx={{ overflowX: 'auto' }}>
        <Table aria-label="Run history" sx={{ minWidth: 860 }}>
          <TableHead>
            <TableRow>
              <SortableHeader label="Status" sortKey="status" sort={sort} onSort={handleSort} />
              <SortableHeader label="Attempt" sortKey="attempt" sort={sort} onSort={handleSort} />
              <SortableHeader label="Available" sortKey="availableAt" sort={sort} onSort={handleSort} />
              <SortableHeader label="Started" sortKey="startedAt" sort={sort} onSort={handleSort} />
              <SortableHeader label="Finished" sortKey="finishedAt" sort={sort} onSort={handleSort} />
              <SortableHeader label="Duration" sortKey="durationMillis" sort={sort} onSort={handleSort} />
              <TableCell align="right">Details</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {sortedRuns.map(run => <RunRow key={run.id} run={run} />)}
            {runs.length === 0 && (
              <TableRow>
                <TableCell colSpan={7}>
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
