import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import {
  Alert,
  Button,
  Checkbox,
  Collapse,
  FormControlLabel,
  ListItemText,
  MenuItem,
  Box,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useId, useState, type ChangeEvent, type FormEvent } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import DatasourceSelector from '../components/DatasourceSelector';
import DataFilteringTabs from '../components/DataFilteringTabs';
import StagingOptionsTabs, { type StagingTarget } from '../components/StagingOptionsTabs';
import LoadingState from '../components/LoadingState';
import OperationalNotice from '../components/OperationalNotice';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';
import {
  createJob,
  COMPLETE_MODE_WARNING,
  getJob,
  toJobDefinitionRequest,
  updateJob,
  type JobDefinitionFormInput,
  type JobDefinitionResponse
} from '../api/jobsApi';
import type { components } from '../api/schema';

type ReplicationMode = JobDefinitionFormInput['mode'];
type JobDefinitionRequest = components['schemas']['JobDefinitionRequest'];

const replicationModeValues: ReplicationMode[] = ['complete', 'complete-atomic', 'incremental'];
const replicationModeDetails: Record<ReplicationMode, { label: string; description: string }> = {
  complete: {
    label: 'Complete mode',
    description: 'Replaces the sink before loading.'
  },
  'complete-atomic': {
    label: 'Complete atomic mode',
    description: 'All-or-nothing load when supported.'
  },
  incremental: {
    label: 'Incremental mode',
    description: 'Loads changes from a watermark column.'
  }
};

type StringField = Exclude<keyof JobDefinitionFormInput,
  | 'sourceDatasourceId'
  | 'sourceDatasourceUseEnabled'
  | 'sinkDatasourceId'
  | 'sinkDatasourceUseEnabled'
  | 'jobs'
  | 'mode'
  | 'fetchSize'
  | 'bandwidthThrottling'
  | 'verbose'
  | 'maxAttempts'
  | 'retryBackoffSeconds'
  | 'automaticRetryEnabled'
  | 'sinkDisableEscape'
  | 'sinkDisableTruncate'>;
type FormErrors = Partial<Record<
  StringField | 'sourceDatasourceId' | 'sinkDatasourceId' | 'jobs' | 'maxAttempts' | 'retryBackoffSeconds',
  string
>>;

const emptyForm: JobDefinitionFormInput = {
  name: '',
  sourceDatasourceId: '',
  sourceDatasourceUseEnabled: true,
  sourceTable: '',
  sourceWhere: '',
  sourceColumns: '',
  sourceQuery: '',
  sinkDatasourceId: '',
  sinkDatasourceUseEnabled: true,
  sinkTable: '',
  sinkColumns: '',
  sinkStagingSchema: '',
  sinkStagingTable: '',
  sinkDisableEscape: false,
  sinkDisableTruncate: false,
  mode: 'complete',
  jobs: 1,
  incrementalWatermarkColumn: '',
  initialWatermarkValue: '',
  fetchSize: 100,
  bandwidthThrottling: 0,
  verbose: false,
  maxAttempts: 3,
  retryBackoffSeconds: 60,
  automaticRetryEnabled: false
};

function isReplicationMode(value: string | undefined): value is ReplicationMode {
  return value === 'complete' || value === 'complete-atomic' || value === 'incremental';
}

function defaultAutomaticRetry(mode: ReplicationMode): boolean {
  return mode !== 'complete';
}

function advancedOptionsSummary(form: JobDefinitionFormInput): string {
  const bandwidth = form.bandwidthThrottling > 0
    ? `${form.bandwidthThrottling} KB/s`
    : 'Unlimited bandwidth';
  const retry = form.automaticRetryEnabled ? 'Automatic retry on' : 'Automatic retry off';
  return `Fetch size ${form.fetchSize} | ${bandwidth} | ${form.maxAttempts} total attempts | ${retry}`;
}

function requestForStagingTarget(form: JobDefinitionFormInput, target: StagingTarget): JobDefinitionRequest {
  const request = toJobDefinitionRequest(form);
  if (target === 'schema') {
    delete request.sinkStagingTable;
  } else {
    delete request.sinkStagingSchema;
  }
  return request;
}

function formFromJob(job: JobDefinitionResponse): JobDefinitionFormInput {
  return {
    name: job.name ?? '',
    sourceDatasourceId: job.sourceDatasourceId ?? '',
    sourceDatasourceUseEnabled: job.sourceDatasourceUseEnabled ?? true,
    sourceTable: job.sourceTable ?? '',
    sourceWhere: job.sourceWhere ?? '',
    sourceColumns: job.sourceColumns ?? '',
    sourceQuery: job.sourceQuery ?? '',
    sinkDatasourceId: job.sinkDatasourceId ?? '',
    sinkDatasourceUseEnabled: job.sinkDatasourceUseEnabled ?? true,
    sinkTable: job.sinkTable ?? '',
    sinkColumns: job.sinkColumns ?? '',
    sinkStagingSchema: job.sinkStagingSchema ?? '',
    sinkStagingTable: job.sinkStagingTable ?? '',
    sinkDisableEscape: job.sinkDisableEscape ?? false,
    sinkDisableTruncate: job.sinkDisableTruncate ?? false,
    mode: isReplicationMode(job.mode) ? job.mode : 'complete',
    jobs: job.jobs ?? 1,
    incrementalWatermarkColumn: job.incrementalWatermarkColumn ?? '',
    initialWatermarkValue: job.initialWatermarkValue ?? '',
    fetchSize: job.fetchSize ?? 100,
    bandwidthThrottling: job.bandwidthThrottling ?? 0,
    verbose: job.verbose ?? false,
    maxAttempts: job.maxAttempts ?? 3,
    retryBackoffSeconds: job.retryBackoffSeconds ?? 60,
    automaticRetryEnabled: job.automaticRetryEnabled ?? defaultAutomaticRetry(
      isReplicationMode(job.mode) ? job.mode : 'complete'
    )
  };
}

function validateForm(form: JobDefinitionFormInput, editMode: boolean): FormErrors {
  const errors: FormErrors = {};
  if (!editMode && !form.name.trim()) {
    errors.name = 'Name is required.';
  }
  if (!form.sourceDatasourceId.trim()) {
    errors.sourceDatasourceId = 'Source datasource is required.';
  }
  if (!form.sourceTable.trim() && !form.sourceQuery?.trim()) {
    errors.sourceTable = 'Source table or query is required.';
  }
  if (!form.sinkDatasourceId.trim()) {
    errors.sinkDatasourceId = 'Sink datasource is required.';
  }
  if (!form.sinkTable.trim()) {
    errors.sinkTable = 'Sink table is required.';
  }
  if (form.jobs < 1) {
    errors.jobs = 'Parallelism must be at least 1.';
  }
  if (form.mode === 'incremental' && !form.incrementalWatermarkColumn?.trim()) {
    errors.incrementalWatermarkColumn = 'Watermark column is required for incremental mode.';
  }
  if (!Number.isFinite(form.maxAttempts) || form.maxAttempts < 1) {
    errors.maxAttempts = 'Maximum attempts must be at least 1.';
  }
  if (!Number.isFinite(form.retryBackoffSeconds) || form.retryBackoffSeconds < 0) {
    errors.retryBackoffSeconds = 'Retry backoff cannot be negative.';
  }
  return errors;
}

function mutationErrorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to save this job.';
}

export default function JobFormPage() {
  const { id } = useParams<{ id: string }>();
  const editMode = Boolean(id);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form, setForm] = useState<JobDefinitionFormInput>(emptyForm);
  const [errors, setErrors] = useState<FormErrors>({});
  const [errorMessage, setErrorMessage] = useState<string>();
  const [retryPolicyTouched, setRetryPolicyTouched] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [stagingTarget, setStagingTarget] = useState<StagingTarget>('schema');
  const advancedOptionsId = useId();

  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: editMode
  });

  useEffect(() => {
    if (jobQuery.data) {
      setForm(formFromJob(jobQuery.data));
      setRetryPolicyTouched(false);
      setStagingTarget(jobQuery.data.sinkStagingTable ? 'table' : 'schema');
    }
  }, [jobQuery.data]);

  const mutation = useMutation({
    mutationFn: (request: JobDefinitionRequest) =>
      editMode && id ? updateJob(id, request) : createJob(request),
    onSuccess: async result => {
      await queryClient.invalidateQueries({ queryKey: ['jobs'] });
      if (editMode && id) {
        await queryClient.invalidateQueries({ queryKey: ['jobs', id] });
      }
      if (result.id) {
        navigate(`/jobs/${result.id}`);
      }
    },
    onError: error => {
      setErrorMessage(mutationErrorMessage(error));
    }
  });

  if (editMode && jobQuery.isPending) {
    return <LoadingState label="Loading job" />;
  }

  if (editMode && (jobQuery.isError || !jobQuery.data)) {
    return <Alert severity="error">Unable to load this job.</Alert>;
  }

  const updateStringField = (field: StringField) => (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    setForm(current => ({ ...current, [field]: event.target.value }));
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextErrors = validateForm(form, editMode);
    setErrors(nextErrors);
    setErrorMessage(undefined);
    if (nextErrors.maxAttempts || nextErrors.retryBackoffSeconds) {
      setAdvancedOpen(true);
    }
    if (Object.keys(nextErrors).length > 0) {
      return;
    }
    mutation.mutate(requestForStagingTarget(form, stagingTarget));
  };

  const modeWarning = form.mode === 'complete'
    ? jobQuery.data?.modeWarning ?? COMPLETE_MODE_WARNING
    : undefined;
  const selectedMode = replicationModeDetails[form.mode];

  return (
    <Stack spacing={3}>
      <PageHeader
        title={editMode ? 'Edit job' : 'New job'}
        description={editMode ? 'Update the replication definition.' : 'Create a replication definition.'}
        backLink={
          <Button
            component={RouterLink}
            to={editMode && id ? `/jobs/${id}` : '/'}
            variant="text"
            startIcon={<ArrowBackIcon />}
          >
            {editMode ? 'Back to job' : 'Back to jobs'}
          </Button>
        }
      />
      <Stack spacing={1}>
        {modeWarning && <OperationalNotice severity="warning">{modeWarning}</OperationalNotice>}
        {errorMessage && <OperationalNotice severity="error">{errorMessage}</OperationalNotice>}
      </Stack>
      <Box component="form" noValidate onSubmit={submit}>
        <Stack spacing={2.5}>
          <SurfaceSection title="Basics" description="Name the job and choose its replication mode.">
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '2fr 1fr' }, gap: 2 }}>
                <TextField
                  label={editMode ? 'Job name' : 'Name'}
                  value={form.name}
                  onChange={updateStringField('name')}
                  disabled={editMode}
                  required={!editMode}
                  error={Boolean(errors.name)}
                  helperText={editMode ? 'Job names cannot be changed after creation.' : errors.name}
                  fullWidth
                />
                <TextField
                  select
                  label="Mode"
                  value={form.mode}
                  helperText={selectedMode.description}
                  SelectProps={{
                    renderValue: value => replicationModeDetails[value as ReplicationMode]?.label ?? String(value)
                  }}
                  onChange={event => setForm(current => {
                    const mode = event.target.value as ReplicationMode;
                    return {
                      ...current,
                      mode,
                      automaticRetryEnabled: retryPolicyTouched
                        ? current.automaticRetryEnabled
                        : defaultAutomaticRetry(mode)
                    };
                  })}
                  fullWidth
                >
                  {replicationModeValues.map(mode => (
                    <MenuItem key={mode} value={mode}>
                      <ListItemText
                        primary={replicationModeDetails[mode].label}
                        secondary={replicationModeDetails[mode].description}
                        primaryTypographyProps={{ fontWeight: 500, color: 'text.primary' }}
                        secondaryTypographyProps={{ color: 'text.secondary' }}
                      />
                    </MenuItem>
                  ))}
                </TextField>
            </Box>
          </SurfaceSection>

          <SurfaceSection title="Source" description="Choose where ReplicaDB reads data.">
            <Stack spacing={2}>
                <DatasourceSelector
                  side="source"
                  value={form.sourceDatasourceId}
                  selectedSummary={jobQuery.data?.sourceDatasource}
                  onChange={sourceDatasourceId => setForm(current => ({ ...current, sourceDatasourceId }))}
                  error={errors.sourceDatasourceId}
                />
                <FormControlLabel
                  control={<Checkbox
                    checked={form.sourceDatasourceUseEnabled}
                    inputProps={{ 'aria-label': 'Source binding enabled' }}
                    onChange={event => setForm(current => ({
                      ...current,
                      sourceDatasourceUseEnabled: event.target.checked
                    }))}
                  />}
                  label={<Box>
                    <Typography variant="body2">Source binding enabled</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Disable to block future manual and scheduled runs; active work is not cancelled.
                    </Typography>
                  </Box>}
                />
                <DataFilteringTabs
                  values={{
                    table: form.sourceTable,
                    columns: form.sourceColumns ?? '',
                    where: form.sourceWhere ?? '',
                    query: form.sourceQuery ?? ''
                  }}
                    onChange={(field, value) => setForm(current => ({
                      ...current,
                    sourceTable: field === 'table' ? value : current.sourceTable,
                    sourceColumns: field === 'columns' ? value : current.sourceColumns,
                    sourceWhere: field === 'where' ? value : current.sourceWhere,
                    sourceQuery: field === 'query' ? value : current.sourceQuery
                    }))}
                  sourceType="custom"
                  fileParams={{}}
                  onFileParamChange={() => undefined}
                  tableError={errors.sourceTable}
                />
            </Stack>
          </SurfaceSection>

          <SurfaceSection title="Sink" description="Choose where ReplicaDB writes data.">
            <Stack spacing={2}>
                <DatasourceSelector
                  side="sink"
                  value={form.sinkDatasourceId}
                  selectedSummary={jobQuery.data?.sinkDatasource}
                  onChange={sinkDatasourceId => setForm(current => ({ ...current, sinkDatasourceId }))}
                  error={errors.sinkDatasourceId}
                />
                <FormControlLabel
                  control={<Checkbox
                    checked={form.sinkDatasourceUseEnabled}
                    inputProps={{ 'aria-label': 'Sink binding enabled' }}
                    onChange={event => setForm(current => ({
                      ...current,
                      sinkDatasourceUseEnabled: event.target.checked
                    }))}
                  />}
                  label={<Box>
                    <Typography variant="body2">Sink binding enabled</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Disable to block future manual and scheduled runs; active work is not cancelled.
                    </Typography>
                  </Box>}
                />
                <Typography component="h3" variant="subtitle1" fontWeight={700}>
                  Data mapping
                </Typography>
                <TextField
                  label="Sink table"
                  value={form.sinkTable}
                  onChange={updateStringField('sinkTable')}
                  required
                  error={Boolean(errors.sinkTable)}
                  helperText={errors.sinkTable ?? 'Table to populate'}
                  fullWidth
                />
                <TextField
                  label="Sink columns"
                  value={form.sinkColumns ?? ''}
                  onChange={updateStringField('sinkColumns')}
                  helperText="Comma-delimited columns to populate"
                  fullWidth
                />
                <StagingOptionsTabs
                  schema={form.sinkStagingSchema ?? ''}
                  table={form.sinkStagingTable ?? ''}
                  target={stagingTarget}
                  onTargetChange={setStagingTarget}
                  onChange={(field, value) => setForm(current => ({
                    ...current,
                    sinkStagingSchema: field === 'schema' ? value : current.sinkStagingSchema,
                    sinkStagingTable: field === 'table' ? value : current.sinkStagingTable
                  }))}
                />
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1 }}>
                  <FormControlLabel
                    control={<Checkbox
                      checked={!form.sinkDisableEscape}
                      onChange={event => setForm(current => ({
                        ...current,
                        sinkDisableEscape: !event.target.checked
                      }))}
                    />}
                    label={<Box><Typography variant="body2">Escape values</Typography><Typography variant="caption" color="text.secondary">Escape strings before inserting</Typography></Box>}
                  />
                  <FormControlLabel
                    control={<Checkbox
                      checked={!form.sinkDisableTruncate}
                      onChange={event => setForm(current => ({
                        ...current,
                        sinkDisableTruncate: !event.target.checked
                      }))}
                    />}
                    label={<Box><Typography variant="body2">Truncate sink table</Typography><Typography variant="caption" color="text.secondary">Clear the sink table before loading</Typography></Box>}
                  />
                </Box>
            </Stack>
          </SurfaceSection>

          <SurfaceSection title="Watermark and execution" description="Set parallelism and resume behavior. Optional tuning is under Advanced options.">
            <Stack spacing={2}>
              <Box sx={{ maxWidth: { sm: 320 } }}>
                <TextField
                  label="Parallel tasks"
                  type="number"
                  value={form.jobs}
                  onChange={event => setForm(current => ({
                    ...current,
                    jobs: event.target.value === '' ? 0 : Number(event.target.value)
                  }))}
                  inputProps={{ min: 1 }}
                  required
                  error={Boolean(errors.jobs)}
                  helperText={errors.jobs}
                  fullWidth
                />
              </Box>
              <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
                <TextField
                  label="Incremental watermark column"
                  value={form.incrementalWatermarkColumn ?? ''}
                  onChange={updateStringField('incrementalWatermarkColumn')}
                  disabled={form.mode !== 'incremental'}
                  required={form.mode === 'incremental'}
                  error={Boolean(errors.incrementalWatermarkColumn)}
                  helperText={errors.incrementalWatermarkColumn}
                  fullWidth
                />
                <TextField
                  label="Initial watermark value"
                  value={form.initialWatermarkValue ?? ''}
                  onChange={updateStringField('initialWatermarkValue')}
                  disabled={form.mode !== 'incremental'}
                  fullWidth
                />
              </Box>
              <Box sx={{ borderTop: 1, borderColor: 'divider', pt: 1 }}>
                <Stack
                  direction={{ xs: 'column', sm: 'row' }}
                  spacing={{ xs: 0.5, sm: 2 }}
                  alignItems={{ xs: 'stretch', sm: 'center' }}
                >
                  <Button
                    type="button"
                    variant="text"
                    aria-expanded={advancedOpen}
                    aria-controls={advancedOptionsId}
                    onClick={() => setAdvancedOpen(current => !current)}
                    endIcon={(
                      <ExpandMoreIcon
                        sx={{
                          transform: advancedOpen ? 'rotate(180deg)' : 'none',
                          transition: 'transform 150ms ease'
                        }}
                      />
                    )}
                    sx={{ alignSelf: { xs: 'flex-start', sm: 'center' }, px: 1 }}
                  >
                    Advanced options
                  </Button>
                  <Typography color="text.secondary" variant="body2" sx={{ minWidth: 0 }}>
                    {advancedOptionsSummary(form)}
                  </Typography>
                </Stack>
                <Collapse in={advancedOpen} timeout="auto" unmountOnExit>
                  <Stack id={advancedOptionsId} spacing={2.5} sx={{ pt: 2 }}>
                    <Typography color="text.secondary" variant="body2">
                      Optional tuning for throughput, diagnostics, and worker lease recovery.
                    </Typography>
                    <Box>
                      <Typography component="h3" variant="subtitle1" fontWeight={700}>
                        Performance
                      </Typography>
                      <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5, mb: 1.5 }}>
                        Control how much data each read fetches and whether transfer speed is capped.
                      </Typography>
                      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
                        <TextField
                          label="Fetch size"
                          type="number"
                          value={form.fetchSize}
                          onChange={event => setForm(current => ({
                            ...current,
                            fetchSize: event.target.value === '' ? 0 : Number(event.target.value)
                          }))}
                          inputProps={{ min: 1 }}
                          fullWidth
                        />
                        <TextField
                          label="Bandwidth (KB/s)"
                          type="number"
                          value={form.bandwidthThrottling}
                          onChange={event => setForm(current => ({
                            ...current,
                            bandwidthThrottling: event.target.value === '' ? 0 : Number(event.target.value)
                          }))}
                          inputProps={{ min: 0 }}
                          fullWidth
                        />
                      </Box>
                    </Box>
                    <Box sx={{ borderTop: 1, borderColor: 'divider', pt: 2 }}>
                      <Typography component="h3" variant="subtitle1" fontWeight={700}>
                        Diagnostics
                      </Typography>
                      <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5 }}>
                        Include detailed replication output when troubleshooting a run.
                      </Typography>
                      <FormControlLabel
                        sx={{ mt: 1 }}
                        control={<Checkbox
                          checked={form.verbose}
                          onChange={event => setForm(current => ({ ...current, verbose: event.target.checked }))}
                        />}
                        label="Verbose logging"
                      />
                    </Box>
                    <Box sx={{ borderTop: 1, borderColor: 'divider', pt: 2 }}>
                      <Typography component="h3" variant="subtitle1" fontWeight={700}>
                        Retry policy
                      </Typography>
                      <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5, mb: 1.5 }}>
                        A lease expiry creates a new attempt; interrupted work is not resumed.
                      </Typography>
                      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
                        <TextField
                          label="Maximum automatic attempts"
                          type="number"
                          value={form.maxAttempts}
                          onChange={event => {
                            setRetryPolicyTouched(true);
                            setForm(current => ({
                              ...current,
                              maxAttempts: event.target.value === '' ? 0 : Number(event.target.value)
                            }));
                          }}
                          inputProps={{ min: 1 }}
                          required
                          error={Boolean(errors.maxAttempts)}
                          helperText={errors.maxAttempts ?? 'Includes the initial attempt.'}
                          fullWidth
                        />
                        <TextField
                          label="Retry delay after lease expiry (seconds)"
                          type="number"
                          value={form.retryBackoffSeconds}
                          onChange={event => {
                            setRetryPolicyTouched(true);
                            setForm(current => ({
                              ...current,
                              retryBackoffSeconds: event.target.value === '' ? 0 : Number(event.target.value)
                            }));
                          }}
                          inputProps={{ min: 0 }}
                          required
                          error={Boolean(errors.retryBackoffSeconds)}
                          helperText={errors.retryBackoffSeconds ?? 'Wait before starting another attempt.'}
                          fullWidth
                        />
                      </Box>
                      <FormControlLabel
                        sx={{ mt: 1 }}
                        control={<Checkbox
                          checked={form.automaticRetryEnabled}
                          onChange={event => {
                            setRetryPolicyTouched(true);
                            setForm(current => ({ ...current, automaticRetryEnabled: event.target.checked }));
                          }}
                        />}
                        label="Retry automatically after a worker lease expires"
                      />
                      <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5 }}>
                        Automatic retry defaults on for incremental and complete atomic modes, and off for complete mode because complete mode can clear the sink.
                      </Typography>
                    </Box>
                  </Stack>
                </Collapse>
              </Box>
            </Stack>
          </SurfaceSection>
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', flexDirection: { xs: 'column', sm: 'row' }, gap: 1 }}>
            <Button type="submit" variant="contained" disabled={mutation.isPending} sx={{ width: { xs: '100%', sm: 'auto' } }}>
              {mutation.isPending ? 'Saving...' : editMode ? 'Save changes' : 'Create job'}
            </Button>
          </Box>
        </Stack>
      </Box>
    </Stack>
  );
}
