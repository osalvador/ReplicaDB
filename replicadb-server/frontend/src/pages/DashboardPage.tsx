import RefreshIcon from '@mui/icons-material/Refresh';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import {
  Alert,
  Box,
  Button,
  FormHelperText,
  Popover,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Typography
} from '@mui/material';
import { useState, type MouseEvent } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getDashboardSummary, type DashboardWindow } from '../api/dashboardApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

type RangeKey = '1h' | '24h' | '7d' | '15d' | '30d' | 'custom';
type DurationUnit = 'hours' | 'days';
const rangeOptions: Array<{ value: RangeKey; label: string; hours: number }> = [
  { value: '1h', label: '1h', hours: 1 },
  { value: '24h', label: '24h', hours: 24 },
  { value: '7d', label: '7d', hours: 24 * 7 },
  { value: '15d', label: '15d', hours: 24 * 15 },
  { value: '30d', label: '30d', hours: 24 * 30 }
];

function createWindow(range: RangeKey): DashboardWindow {
  const to = new Date();
  const option = rangeOptions.find(item => item.value === range) ?? rangeOptions[1];
  const from = new Date(to.getTime() - option.hours * 60 * 60 * 1000);
  return { from: from.toISOString(), to: to.toISOString() };
}

function createCustomWindow(duration: number, unit: DurationUnit): DashboardWindow | undefined {
  if (!Number.isFinite(duration) || duration <= 0) {
    return undefined;
  }
  const to = new Date();
  const hours = unit === 'days' ? duration * 24 : duration;
  const from = new Date(to.getTime() - hours * 60 * 60 * 1000);
  return { from: from.toISOString(), to: to.toISOString() };
}

function customRangeLabel(duration: number, unit: DurationUnit): string {
  return unit === 'days' ? `Last ${duration}d` : `Last ${duration}h`;
}

function formatNumber(value: number | null | undefined): string {
  return new Intl.NumberFormat().format(value ?? 0);
}

function formatMillis(value: number | null | undefined): string {
  const millis = value ?? 0;
  return millis < 1000 ? `${millis} ms` : `${(millis / 1000).toFixed(millis >= 10000 ? 0 : 1)} s`;
}

function formatBucket(value: string | undefined): string {
  return value
    ? new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: 'numeric' }).format(new Date(value))
    : 'Unknown';
}

function Metric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <Box sx={{ borderBottom: 1, borderColor: 'divider', pb: 1.5, minWidth: 0 }}>
      <Typography variant="overline" color="text.secondary" sx={{ letterSpacing: '0.06em' }}>{label}</Typography>
      <Typography variant="h5" fontWeight={700} sx={{ mt: 0.25 }}>{value}</Typography>
      {detail && <Typography variant="body2" color="text.secondary">{detail}</Typography>}
    </Box>
  );
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <Stack direction="row" spacing={0.5} alignItems="center">
      <Box aria-hidden="true" sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: color }} />
      <Typography variant="caption" color="text.secondary">{label}</Typography>
    </Stack>
  );
}

function OutcomeBars({ outcomes }: { outcomes: Array<{ bucket?: string; succeeded: number; failed: number; active: number }> }) {
  const maxValue = Math.max(1, ...outcomes.flatMap(point => [point.succeeded, point.failed, point.active]));
  if (outcomes.length === 0) {
    return <EmptyState title="No runs in this window." description="Choose a wider time range to see outcomes." />;
  }

  return (
    <Stack component="ul" spacing={1.5} sx={{ listStyle: 'none', p: 0, m: 0 }}>
      {outcomes.map(point => (
        <Box component="li" key={point.bucket}>
          <Stack direction="row" justifyContent="space-between" sx={{ mb: 0.5 }}>
            <Typography variant="body2" fontWeight={700}>{formatBucket(point.bucket)}</Typography>
            <Typography variant="body2" color="text.secondary">{point.succeeded + point.failed + point.active} runs</Typography>
          </Stack>
          <Stack direction="row" spacing={1} alignItems="center">
            {([['Succeeded', point.succeeded, 'success.main'], ['Failed', point.failed, 'error.main'], ['Active', point.active, 'info.main']] as const).map(([label, value, color]) => (
              <Box key={label} role="img" aria-label={`${label}: ${value}`} sx={{ height: 8, flex: Math.max(0.08, value / maxValue), minWidth: 8, borderRadius: 1, bgcolor: color }} />
            ))}
          </Stack>
        </Box>
      ))}
      <Stack direction="row" spacing={2} sx={{ pt: 0.5 }}>
        <Legend color="success.main" label="Succeeded" />
        <Legend color="error.main" label="Failed" />
        <Legend color="info.main" label="Active" />
      </Stack>
    </Stack>
  );
}

function PerformanceBars({ performance }: { performance: Array<{ jobId?: string; jobName?: string; averageDurationMillis?: number; averageLatencyMillis?: number }> }) {
  if (performance.length === 0) {
    return <EmptyState title="No job performance data." description="Run data will appear here for the selected window." />;
  }
  const maxDuration = Math.max(1, ...performance.map(item => item.averageDurationMillis ?? 0));
  const maxLatency = Math.max(1, ...performance.map(item => item.averageLatencyMillis ?? 0));
  return (
    <Stack component="ul" spacing={1.5} sx={{ listStyle: 'none', p: 0, m: 0 }}>
      {performance.map(item => (
        <Box component="li" key={item.jobId}>
          <Stack direction="row" justifyContent="space-between" spacing={2} sx={{ mb: 0.5 }}>
            <Typography variant="body2" fontWeight={700} noWrap>{item.jobName ?? 'Unnamed job'}</Typography>
            <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>{formatMillis(item.averageDurationMillis)} avg</Typography>
          </Stack>
          <Stack spacing={0.5}>
            {([['Duration', item.averageDurationMillis ?? 0, maxDuration, 'secondary.main'], ['Queue latency', item.averageLatencyMillis ?? 0, maxLatency, 'primary.main']] as const).map(([label, value, max, color]) => (
              <Stack key={label} direction="row" spacing={1} alignItems="center">
                <Typography variant="caption" color="text.secondary" sx={{ width: 82, flexShrink: 0 }}>{label}</Typography>
                <Box sx={{ height: 7, flex: 1, bgcolor: 'action.hover', borderRadius: 1, overflow: 'hidden' }}>
                  <Box role="img" aria-label={`${label}: ${formatMillis(value)}`} sx={{ width: `${Math.max(3, value / max * 100)}%`, height: '100%', bgcolor: color }} />
                </Box>
                <Typography variant="caption" sx={{ width: 56, textAlign: 'right' }}>{formatMillis(value)}</Typography>
              </Stack>
            ))}
          </Stack>
        </Box>
      ))}
      <Stack direction="row" spacing={2} sx={{ pt: 0.5 }}>
        <Legend color="secondary.main" label="Duration" />
        <Legend color="primary.main" label="Queue latency" />
      </Stack>
    </Stack>
  );
}

function normalizeOutcomes(summary: { outcomes?: Array<{ bucket?: string; succeeded?: number; failed?: number; active?: number }> }) {
  return (summary.outcomes ?? []).map(point => ({
    bucket: point.bucket,
    succeeded: point.succeeded ?? 0,
    failed: point.failed ?? 0,
    active: point.active ?? 0
  }));
}

export default function DashboardPage() {
  const [range, setRange] = useState<RangeKey>('24h');
  const [window, setWindow] = useState<DashboardWindow>(() => createWindow('24h'));
  const [customAnchor, setCustomAnchor] = useState<HTMLElement | null>(null);
  const [customDuration, setCustomDuration] = useState(2);
  const [customUnit, setCustomUnit] = useState<DurationUnit>('hours');
  const [customError, setCustomError] = useState(false);
  const summaryQuery = useQuery({
    queryKey: ['dashboardSummary', window],
    queryFn: () => getDashboardSummary(window),
    refetchInterval: 30000
  });
  const selectRange = (_event: MouseEvent<HTMLElement>, value: RangeKey | null) => {
    if (value) {
      setRange(value);
      setWindow(createWindow(value));
    }
  };
  const applyCustomRange = () => {
    const nextWindow = createCustomWindow(customDuration, customUnit);
    if (!nextWindow) {
      setCustomError(true);
      return;
    }
    setCustomError(false);
    setRange('custom');
    setWindow(nextWindow);
    setCustomAnchor(null);
  };

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Dashboard"
        description="A live view of replication health and performance."
        actions={(
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} alignItems={{ xs: 'stretch', sm: 'center' }}>
            <ToggleButtonGroup exclusive size="small" value={range === 'custom' ? null : range} onChange={selectRange} aria-label="Dashboard time window">
              {rangeOptions.map(option => <ToggleButton key={option.value} value={option.value} aria-label={`Last ${option.label}`}>{option.label}</ToggleButton>)}
            </ToggleButtonGroup>
            <Button
              variant={range === 'custom' ? 'contained' : 'outlined'}
              startIcon={<AccessTimeIcon />}
              aria-label={range === 'custom' ? `Custom time range: ${customRangeLabel(customDuration, customUnit)}` : 'Custom time range'}
              onClick={event => setCustomAnchor(event.currentTarget)}
            >
              {range === 'custom' ? customRangeLabel(customDuration, customUnit) : 'Custom'}
            </Button>
            <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => void summaryQuery.refetch()}>Refresh</Button>
            <Popover
              open={Boolean(customAnchor)}
              anchorEl={customAnchor}
              onClose={() => setCustomAnchor(null)}
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
              transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            >
              <Box role="dialog" aria-label="Custom time range" sx={{ p: 2, width: { xs: 300, sm: 360 } }}>
                <Typography variant="h6">Custom time range</Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                  Choose the lookback window for dashboard metrics.
                </Typography>
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} sx={{ mt: 2 }}>
                  <TextField
                    autoFocus
                    label="Duration"
                    type="number"
                    value={customDuration}
                    onChange={event => {
                      setCustomDuration(Number(event.target.value));
                      setCustomError(false);
                    }}
                    inputProps={{ min: 1, max: 365, step: 1 }}
                    error={customError}
                    fullWidth
                  />
                  <ToggleButtonGroup
                    exclusive
                    value={customUnit}
                    onChange={(_event, value: DurationUnit | null) => value && setCustomUnit(value)}
                    aria-label="Custom time unit"
                    size="small"
                    sx={{ alignSelf: { xs: 'stretch', sm: 'center' } }}
                  >
                    <ToggleButton value="hours" aria-label="Hours">Hours</ToggleButton>
                    <ToggleButton value="days" aria-label="Days">Days</ToggleButton>
                  </ToggleButtonGroup>
                </Stack>
                {customError && <FormHelperText error>Enter a duration greater than 0.</FormHelperText>}
                <Stack direction="row" justifyContent="flex-end" spacing={1} sx={{ mt: 2 }}>
                  <Button onClick={() => setCustomAnchor(null)}>Cancel</Button>
                  <Button variant="contained" onClick={applyCustomRange}>Apply range</Button>
                </Stack>
              </Box>
            </Popover>
          </Stack>
        )}
      />
      {summaryQuery.isPending && <LoadingState label="Loading dashboard" />}
      {summaryQuery.isError && <Alert severity="error" action={<Button color="inherit" size="small" onClick={() => void summaryQuery.refetch()}>Try again</Button>}>Unable to load dashboard metrics. Check the server connection and try again.</Alert>}
      {summaryQuery.data && (
        <>
          <SurfaceSection title="Replication pulse" description={`Runs created from ${new Date(summaryQuery.data.from ?? window.from).toLocaleString()} to ${new Date(summaryQuery.data.to ?? window.to).toLocaleString()}.`}>
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'repeat(2, 1fr)', sm: 'repeat(3, 1fr)', lg: 'repeat(6, 1fr)' }, gap: { xs: 2, md: 3 } }}>
              <Metric label="Jobs" value={formatNumber(summaryQuery.data.totalJobs)} />
              <Metric label="Active runs" value={formatNumber(summaryQuery.data.activeRuns)} detail="Running or cancelling" />
              <Metric label="Success rate" value={`${summaryQuery.data.totalRuns ? Math.round((summaryQuery.data.succeededRuns ?? 0) / summaryQuery.data.totalRuns * 100) : 0}%`} detail={`${formatNumber(summaryQuery.data.totalRuns)} total runs`} />
              <Metric label="Failed runs" value={formatNumber(summaryQuery.data.failedRuns)} detail="Needs attention" />
              <Metric label="Rows processed" value={formatNumber(summaryQuery.data.rowsProcessed)} />
              <Metric label="Avg duration" value={formatMillis(summaryQuery.data.averageDurationMillis)} detail={`Queue ${formatMillis(summaryQuery.data.averageLatencyMillis)}`} />
            </Box>
          </SurfaceSection>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: '1fr 1fr' }, gap: 3 }}>
            <SurfaceSection title="Run outcomes" description="Volume by time bucket in the selected window.">
              <OutcomeBars outcomes={normalizeOutcomes(summaryQuery.data)} />
            </SurfaceSection>
            <SurfaceSection title="Job performance" description="Top jobs by average duration, with queue latency beneath.">
              <PerformanceBars performance={summaryQuery.data.jobPerformance ?? []} />
            </SurfaceSection>
          </Box>
          <SurfaceSection title="Continue operating" description="Move from the overview into a specific resource when a metric needs investigation." actions={<Button component={RouterLink} to="/jobs" variant="outlined">Open jobs</Button>}>
            <Typography variant="body2" color="text.secondary">Use the job catalog to inspect schedules, bindings, and the complete run history behind these measurements.</Typography>
          </SurfaceSection>
        </>
      )}
    </Stack>
  );
}
