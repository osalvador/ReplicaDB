import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import {
  Alert,
  Button,
  Checkbox,
  FormControlLabel,
  MenuItem,
  Box,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useState, type ChangeEvent, type FormEvent } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import DatasourceSelector from '../components/DatasourceSelector';
import DataFilteringTabs from '../components/DataFilteringTabs';
import StagingOptionsTabs from '../components/StagingOptionsTabs';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';
import {
  createJob,
  getJob,
  toJobDefinitionRequest,
  updateJob,
  type JobDefinitionFormInput,
  type JobDefinitionResponse
} from '../api/jobsApi';
import type { components } from '../api/schema';

type ReplicationMode = JobDefinitionFormInput['mode'];
type JobDefinitionRequest = components['schemas']['JobDefinitionRequest'];
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

  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: editMode
  });

  useEffect(() => {
    if (jobQuery.data) {
      setForm(formFromJob(jobQuery.data));
      setRetryPolicyTouched(false);
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
    if (Object.keys(nextErrors).length > 0) {
      return;
    }
    mutation.mutate(toJobDefinitionRequest(form));
  };

  const modeWarning = editMode && form.mode === 'complete' ? jobQuery.data?.modeWarning : undefined;

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
      {modeWarning && <Alert severity="warning">{modeWarning}</Alert>}
      {errorMessage && <Alert severity="error">{errorMessage}</Alert>}
      <Box component="form" noValidate onSubmit={submit}>
        <Stack spacing={2.5}>
          <SurfaceSection title="Basics" description="Name the job and choose its replication mode.">
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '2fr 1fr' }, gap: 2 }}>
                <TextField
                  label="Name"
                  value={form.name}
                  onChange={updateStringField('name')}
                  disabled={editMode}
                  required={!editMode}
                  error={Boolean(errors.name)}
                  helperText={errors.name}
                  fullWidth
                />
                <TextField
                  select
                  label="Mode"
                  value={form.mode}
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
                  <MenuItem value="complete">complete</MenuItem>
                  <MenuItem value="complete-atomic">complete-atomic</MenuItem>
                  <MenuItem value="incremental">incremental</MenuItem>
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
                  onChange={(field, value) => setForm(current => ({
                    ...current,
                    sinkStagingSchema: field === 'schema' ? value : '',
                    sinkStagingTable: field === 'table' ? value : ''
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

          <SurfaceSection title="Watermark and execution" description="Tune parallelism and resume behavior.">
            <Stack spacing={2}>
              <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' }, gap: 2 }}>
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
              <FormControlLabel
                control={<Checkbox
                  checked={form.verbose}
                  onChange={event => setForm(current => ({ ...current, verbose: event.target.checked }))}
                />}
                label="Verbose"
              />
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
                  helperText={errors.maxAttempts ?? 'Includes the initial attempt'}
                  fullWidth
                />
                <TextField
                  label="Retry backoff (seconds)"
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
                  helperText={errors.retryBackoffSeconds ?? 'Delay before lease recovery retry'}
                  fullWidth
                />
              </Box>
              <FormControlLabel
                control={<Checkbox
                  checked={form.automaticRetryEnabled}
                  onChange={event => {
                    setRetryPolicyTouched(true);
                    setForm(current => ({ ...current, automaticRetryEnabled: event.target.checked }));
                  }}
                />}
                label="Automatic retry after lease expiry"
              />
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
