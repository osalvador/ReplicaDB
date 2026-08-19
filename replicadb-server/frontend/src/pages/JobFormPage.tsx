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
import ConnectionSettingsCard, {
  type ConnectionDraft,
  type EndpointField,
  type EndpointValues
} from '../components/ConnectionSettingsCard';
import DataFilteringTabs from '../components/DataFilteringTabs';
import StagingOptionsTabs from '../components/StagingOptionsTabs';
import { FileFormatSettings } from '../components/DataFilteringTabs';
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
import { buildConnectString, parseConnectString, type ConnectionFields } from '../utils/connectionBuilder';

type ReplicationMode = JobDefinitionFormInput['mode'];
type JobDefinitionRequest = components['schemas']['JobDefinitionRequest'];
type StringField = Exclude<keyof JobDefinitionFormInput,
  | 'jobs'
  | 'mode'
  | 'fetchSize'
  | 'bandwidthThrottling'
  | 'verbose'
  | 'sourceConnectionParams'
  | 'sinkConnectionParams'
  | 'sinkDisableEscape'
  | 'sinkDisableTruncate'>;
type FormErrors = Partial<Record<StringField | 'jobs', string>>;

const emptyForm: JobDefinitionFormInput = {
  name: '',
  sourceConnect: '',
  sourceUser: '',
  sourcePassword: '',
  sourceTable: '',
  sourceWhere: '',
  sourceAuthMode: '',
  sourceAuthPrincipalId: '',
  sourceAuthLoginHint: '',
  sourceAuthClientCertificate: '',
  sourceAuthClientKey: '',
  sourceConnectionParams: {},
  sourceColumns: '',
  sourceQuery: '',
  sinkConnect: '',
  sinkUser: '',
  sinkPassword: '',
  sinkTable: '',
  sinkAuthMode: '',
  sinkAuthPrincipalId: '',
  sinkAuthLoginHint: '',
  sinkAuthClientCertificate: '',
  sinkAuthClientKey: '',
  sinkConnectionParams: {},
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
  verbose: false
};

function isReplicationMode(value: string | undefined): value is ReplicationMode {
  return value === 'complete' || value === 'complete-atomic' || value === 'incremental';
}

function formFromJob(job: JobDefinitionResponse): JobDefinitionFormInput {
  return {
    name: job.name ?? '',
    sourceConnect: job.sourceConnect ?? '',
    sourceUser: job.sourceUser ?? '',
    sourcePassword: '',
    sourceTable: job.sourceTable ?? '',
    sourceWhere: job.sourceWhere ?? '',
    sourceAuthMode: job.sourceAuthMode ?? '',
    sourceAuthPrincipalId: job.sourceAuthPrincipalId ?? '',
    sourceAuthLoginHint: job.sourceAuthLoginHint ?? '',
    sourceAuthClientCertificate: job.sourceAuthClientCertificate ?? '',
    sourceAuthClientKey: job.sourceAuthClientKey ?? '',
    sourceConnectionParams: job.sourceConnectionParams ?? {},
    sourceColumns: job.sourceColumns ?? '',
    sourceQuery: job.sourceQuery ?? '',
    sinkConnect: job.sinkConnect ?? '',
    sinkUser: job.sinkUser ?? '',
    sinkPassword: '',
    sinkTable: job.sinkTable ?? '',
    sinkAuthMode: job.sinkAuthMode ?? '',
    sinkAuthPrincipalId: job.sinkAuthPrincipalId ?? '',
    sinkAuthLoginHint: job.sinkAuthLoginHint ?? '',
    sinkAuthClientCertificate: job.sinkAuthClientCertificate ?? '',
    sinkAuthClientKey: job.sinkAuthClientKey ?? '',
    sinkConnectionParams: job.sinkConnectionParams ?? {},
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
    verbose: job.verbose ?? false
  };
}

function validateForm(form: JobDefinitionFormInput, editMode: boolean): FormErrors {
  const errors: FormErrors = {};
  if (!editMode && !form.name.trim()) {
    errors.name = 'Name is required.';
  }
  if (!form.sourceConnect.trim()) {
    errors.sourceConnect = 'Source connection is required.';
  }
  if (!form.sourceTable.trim() && !form.sourceQuery?.trim()) {
    errors.sourceTable = 'Source table or query is required.';
  }
  if (!form.sinkConnect.trim()) {
    errors.sinkConnect = 'Sink connection is required.';
  }
  if (!form.sinkTable.trim()) {
    errors.sinkTable = 'Sink table is required.';
  }
  if (form.jobs < 1) {
    errors.jobs = 'Parallelism must be at least 1.';
  }
  return errors;
}

function mutationErrorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to save this job.';
}

function emptyConnectionDraft(): ConnectionDraft {
  return { type: 'custom', fields: { raw: '' }, extraParams: '' };
}

function connectionDraftFromJob(connect: string, params: Record<string, string>): ConnectionDraft {
  const parsed = parseConnectString(connect);
  const { type, ...fields } = parsed;
  const reservedKeys = new Set([
    'format',
    'format.delimiter',
    'format.quote',
    'format.escape',
    'format.nullString',
    'format.firstRecordAsHeader',
    'format.ignoreEmptyLines',
    'format.ignoreSurroundingSpaces',
    'format.trim',
    'format.recordSeparator',
    'topic',
    'partition',
    'acks'
  ]);
  const extraParams = Object.entries(params)
    .filter(([key]) => !reservedKeys.has(key))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');
  return { type, fields, extraParams };
}

function endpointParams(text: string): Record<string, string> {
  return text.split('\n').reduce<Record<string, string>>((params, line) => {
    const separator = line.indexOf('=');
    if (separator > 0) {
      const key = line.slice(0, separator).trim();
      const value = line.slice(separator + 1).trim();
      if (key && value) {
        params[key] = value;
      }
    }
    return params;
  }, {});
}

function composedConnect(draft: ConnectionDraft, fallback: string): string {
  try {
    return buildConnectString(draft.type, draft.fields);
  } catch {
    return fallback;
  }
}

const endpointFieldMap: Record<'source' | 'sink', Record<EndpointField, StringField>> = {
  source: {
    connect: 'sourceConnect',
    user: 'sourceUser',
    password: 'sourcePassword',
    authMode: 'sourceAuthMode',
    authPrincipalId: 'sourceAuthPrincipalId',
    authLoginHint: 'sourceAuthLoginHint',
    authClientCertificate: 'sourceAuthClientCertificate',
    authClientKey: 'sourceAuthClientKey'
  },
  sink: {
    connect: 'sinkConnect',
    user: 'sinkUser',
    password: 'sinkPassword',
    authMode: 'sinkAuthMode',
    authPrincipalId: 'sinkAuthPrincipalId',
    authLoginHint: 'sinkAuthLoginHint',
    authClientCertificate: 'sinkAuthClientCertificate',
    authClientKey: 'sinkAuthClientKey'
  }
};

function endpointValues(form: JobDefinitionFormInput, side: 'source' | 'sink'): EndpointValues {
  const prefix = side === 'source' ? 'source' : 'sink';
  return {
    connect: form[`${prefix}Connect` as 'sourceConnect' | 'sinkConnect'],
    user: form[`${prefix}User` as 'sourceUser' | 'sinkUser'] ?? '',
    password: form[`${prefix}Password` as 'sourcePassword' | 'sinkPassword'] ?? '',
    authMode: form[`${prefix}AuthMode` as 'sourceAuthMode' | 'sinkAuthMode'] ?? '',
    authPrincipalId: form[`${prefix}AuthPrincipalId` as 'sourceAuthPrincipalId' | 'sinkAuthPrincipalId'] ?? '',
    authLoginHint: form[`${prefix}AuthLoginHint` as 'sourceAuthLoginHint' | 'sinkAuthLoginHint'] ?? '',
    authClientCertificate: form[
      `${prefix}AuthClientCertificate` as 'sourceAuthClientCertificate' | 'sinkAuthClientCertificate'
    ] ?? '',
    authClientKey: form[`${prefix}AuthClientKey` as 'sourceAuthClientKey' | 'sinkAuthClientKey'] ?? ''
  };
}

export default function JobFormPage() {
  const { id } = useParams<{ id: string }>();
  const editMode = Boolean(id);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form, setForm] = useState<JobDefinitionFormInput>(emptyForm);
  const [sourceDraft, setSourceDraft] = useState<ConnectionDraft>(emptyConnectionDraft);
  const [sinkDraft, setSinkDraft] = useState<ConnectionDraft>(emptyConnectionDraft);
  const [errors, setErrors] = useState<FormErrors>({});
  const [errorMessage, setErrorMessage] = useState<string>();

  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: editMode
  });

  useEffect(() => {
    if (jobQuery.data) {
      setForm(formFromJob(jobQuery.data));
      setSourceDraft(connectionDraftFromJob(
        jobQuery.data.sourceConnect ?? '', jobQuery.data.sourceConnectionParams ?? {}));
      setSinkDraft(connectionDraftFromJob(
        jobQuery.data.sinkConnect ?? '', jobQuery.data.sinkConnectionParams ?? {}));
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

  const updateEndpointValue = (side: 'source' | 'sink', field: EndpointField, value: string) => {
    setForm(current => ({ ...current, [endpointFieldMap[side][field]]: value }));
  };

  const updateSourceConnectionParam = (key: string, value: string) => {
    setForm(current => {
      const sourceConnectionParams = { ...(current.sourceConnectionParams ?? {}) };
      if (value) {
        sourceConnectionParams[key] = value;
      } else {
        delete sourceConnectionParams[key];
      }
      return { ...current, sourceConnectionParams };
    });
  };

  const updateSinkConnectionParam = (key: string, value: string) => {
    setForm(current => {
      const sinkConnectionParams = { ...(current.sinkConnectionParams ?? {}) };
      if (value) {
        sinkConnectionParams[key] = value;
      } else {
        delete sinkConnectionParams[key];
      }
      return { ...current, sinkConnectionParams };
    });
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const submissionForm: JobDefinitionFormInput = {
      ...form,
      sourceConnect: composedConnect(sourceDraft, form.sourceConnect),
      sinkConnect: composedConnect(sinkDraft, form.sinkConnect),
      sourceConnectionParams: {
        ...(form.sourceConnectionParams ?? {}),
        ...endpointParams(sourceDraft.extraParams)
      },
      sinkConnectionParams: {
        ...(form.sinkConnectionParams ?? {}),
        ...endpointParams(sinkDraft.extraParams)
      }
    };
    const nextErrors = validateForm(submissionForm, editMode);
    setErrors(nextErrors);
    setErrorMessage(undefined);
    if (Object.keys(nextErrors).length > 0) {
      return;
    }
    mutation.mutate(toJobDefinitionRequest(submissionForm));
  };

  const passwordHelperText = editMode ? 'Leave blank to keep the existing value' : undefined;
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
                  onChange={event => setForm(current => ({ ...current, mode: event.target.value as ReplicationMode }))}
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
                <ConnectionSettingsCard
                  side="source"
                  draft={sourceDraft}
                  values={endpointValues(form, 'source')}
                  onDraftChange={setSourceDraft}
                  onValueChange={(field, value) => updateEndpointValue('source', field, value)}
                  connectError={errors.sourceConnect}
                  passwordHelperText={passwordHelperText}
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
                  sourceType={sourceDraft.type}
                  fileParams={form.sourceConnectionParams ?? {}}
                  onFileParamChange={updateSourceConnectionParam}
                  tableError={errors.sourceTable}
                />
            </Stack>
          </SurfaceSection>

          <SurfaceSection title="Sink" description="Choose where ReplicaDB writes data.">
            <Stack spacing={2}>
                <ConnectionSettingsCard
                  side="sink"
                  draft={sinkDraft}
                  values={endpointValues(form, 'sink')}
                  onDraftChange={setSinkDraft}
                  onValueChange={(field, value) => updateEndpointValue('sink', field, value)}
                  connectionParams={form.sinkConnectionParams}
                  onConnectionParamChange={updateSinkConnectionParam}
                  connectError={errors.sinkConnect}
                  passwordHelperText={passwordHelperText}
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
                {sinkDraft.type === 'file' && (
                  <Box sx={{ borderTop: 1, borderColor: 'divider', pt: 2 }}>
                    <FileFormatSettings
                      values={form.sinkConnectionParams ?? {}}
                      onChange={updateSinkConnectionParam}
                      includeRecordSeparator
                    />
                  </Box>
                )}
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
                  label="Incremental watermark column"
                  value={form.incrementalWatermarkColumn ?? ''}
                  onChange={updateStringField('incrementalWatermarkColumn')}
                  disabled={form.mode !== 'incremental'}
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
