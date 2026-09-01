import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import SaveIcon from '@mui/icons-material/Save';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  FormControlLabel,
  FormGroup,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useState } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import {
  createDatasource,
  datasourceQueryKeys,
  getDatasource,
  invalidateDatasourceQueries,
  updateDatasource,
  type DatasourceMutationInput,
  type DatasourceResponse
} from '../api/datasourcesApi';
import ConnectionSettingsCard, {
  type ConnectionDraft,
  type DatasourceSecurityField,
  type EndpointField,
  type EndpointValues
} from '../components/ConnectionSettingsCard';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';
import {
  buildConnectString,
  parseConnectString,
  type ConnectionFields,
  type DbType
} from '../utils/connectionBuilder';

type DatasourceFormState = {
  name: string;
  connectorType: DbType;
  draft: ConnectionDraft;
  technicalParams: Record<string, string>;
  security: Record<string, string>;
  clearSecurityKeys: string[];
  connectionEdited: boolean;
};

type FormErrors = {
  name?: string;
  connect?: string;
};

const initialDraft: ConnectionDraft = {
  type: 'postgres',
  fields: { host: '', port: '5432', database: '' },
  extraParams: ''
};

const emptyEndpointValues: EndpointValues = {
  connect: '',
  user: '',
  password: '',
  authMode: '',
  authPrincipalId: '',
  authLoginHint: '',
  authClientCertificate: '',
  authClientKey: ''
};

const clearOptions: Array<{ key: string; label: string }> = [
  { key: 'password', label: 'User password' },
  { key: 'connect.parameter.accessKey', label: 'S3 access key' },
  { key: 'connect.parameter.secretKey', label: 'S3 secret key' },
  { key: 'connect.parameter.sasl.username', label: 'Kafka SASL username' },
  { key: 'connect.parameter.sasl.password', label: 'Kafka SASL password' },
  { key: 'connect.parameter.ssl.truststore.password', label: 'Kafka truststore password' },
  { key: 'connect.parameter.ssl.keystore.password', label: 'Kafka keystore password' },
  { key: 'auth.mode', label: 'Azure authentication mode' },
  { key: 'auth.principal.id', label: 'Azure principal ID' },
  { key: 'auth.login.hint', label: 'Azure login hint' },
  { key: 'auth.client.certificate', label: 'Azure client certificate' },
  { key: 'auth.client.key', label: 'Azure client key' }
];

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to save this datasource.';
}

function parseTechnicalParams(text: string): Record<string, string> {
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

function formatTechnicalParams(params: Record<string, string>): string {
  return Object.entries(params)
    .sort(([first], [second]) => first.localeCompare(second))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');
}

function connectorType(value: string | undefined): DbType {
  const supported: DbType[] = [
    'oracle', 'mysql', 'mariadb', 'postgres', 'db2', 'db2i', 'sqlite', 'sqlserver',
    'denodo', 'file', 'kafka', 's3', 'mongodb', 'mongodb+srv', 'custom'
  ];
  return value && supported.includes(value as DbType) ? value as DbType : 'custom';
}

function connectionPreview(draft: ConnectionDraft): string {
  try {
    return buildConnectString(draft.type, draft.fields);
  } catch {
    return draft.type === 'custom' ? draft.fields.raw ?? '' : '';
  }
}

function formFromDatasource(datasource: DatasourceResponse): DatasourceFormState {
  const type = connectorType(datasource.connectorType);
  const parsed = datasource.safeConnectDisplay
    ? parseConnectString(datasource.safeConnectDisplay)
    : undefined;
  const fields: ConnectionFields = parsed?.type === type ? parsed : {};
  const draft: ConnectionDraft = {
    type,
    fields,
    extraParams: formatTechnicalParams(datasource.technicalParams ?? {})
  };
  return {
    name: datasource.name ?? '',
    connectorType: type,
    draft,
    technicalParams: datasource.technicalParams ?? {},
    security: {},
    clearSecurityKeys: [],
    connectionEdited: false
  };
}

function securityKey(field: EndpointField): string {
  switch (field) {
    case 'connect': return 'connect';
    case 'user': return 'user';
    case 'password': return 'password';
    case 'authMode': return 'auth.mode';
    case 'authPrincipalId': return 'auth.principal.id';
    case 'authLoginHint': return 'auth.login.hint';
    case 'authClientCertificate': return 'auth.client.certificate';
    case 'authClientKey': return 'auth.client.key';
  }
}

function datasourceSecurityKey(field: DatasourceSecurityField): string {
  switch (field) {
    case 'accessKey': return 'connect.parameter.accessKey';
    case 'secretKey': return 'connect.parameter.secretKey';
    case 'saslUsername': return 'connect.parameter.sasl.username';
    case 'saslPassword': return 'connect.parameter.sasl.password';
    case 'sslTruststorePassword': return 'connect.parameter.ssl.truststore.password';
    case 'sslKeystorePassword': return 'connect.parameter.ssl.keystore.password';
  }
}

function endpointValues(security: Record<string, string>): EndpointValues {
  return {
    ...emptyEndpointValues,
    connect: security.connect ?? '',
    user: security.user ?? '',
    password: security.password ?? '',
    authMode: security['auth.mode'] ?? '',
    authPrincipalId: security['auth.principal.id'] ?? '',
    authLoginHint: security['auth.login.hint'] ?? '',
    authClientCertificate: security['auth.client.certificate'] ?? '',
    authClientKey: security['auth.client.key'] ?? ''
  };
}

function securityValues(security: Record<string, string>): Partial<Record<DatasourceSecurityField, string>> {
  return {
    accessKey: security['connect.parameter.accessKey'] ?? '',
    secretKey: security['connect.parameter.secretKey'] ?? '',
    saslUsername: security['connect.parameter.sasl.username'] ?? '',
    saslPassword: security['connect.parameter.sasl.password'] ?? '',
    sslTruststorePassword: security['connect.parameter.ssl.truststore.password'] ?? '',
    sslKeystorePassword: security['connect.parameter.ssl.keystore.password'] ?? ''
  };
}

export default function DatasourceFormPage() {
  const { id } = useParams<{ id: string }>();
  const editMode = Boolean(id);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form, setForm] = useState<DatasourceFormState>({
    name: '',
    connectorType: initialDraft.type,
    draft: initialDraft,
    technicalParams: {},
    security: {},
    clearSecurityKeys: [],
    connectionEdited: false
  });
  const [errors, setErrors] = useState<FormErrors>({});
  const [errorMessageText, setErrorMessageText] = useState<string>();

  const datasourceQuery = useQuery({
    queryKey: datasourceQueryKeys.detail(id ?? ''),
    queryFn: () => getDatasource(id ?? ''),
    enabled: editMode
  });

  useEffect(() => {
    if (datasourceQuery.data) {
      setForm(formFromDatasource(datasourceQuery.data));
      setErrors({});
      setErrorMessageText(undefined);
    }
  }, [datasourceQuery.data]);

  const mutation = useMutation({
    mutationFn: (input: DatasourceMutationInput) => editMode && id
      ? updateDatasource(id, input)
      : createDatasource(input),
    onSuccess: result => {
      void invalidateDatasourceQueries(queryClient);
      if (result.id) {
        navigate(`/datasources/${result.id}`);
      } else {
        navigate('/datasources');
      }
    },
    onError: error => {
      setErrorMessageText(errorMessage(error));
    }
  });

  if (editMode && datasourceQuery.isPending) {
    return <LoadingState label="Loading datasource" />;
  }

  if (editMode && (datasourceQuery.isError || !datasourceQuery.data)) {
    return <Alert severity="error">{errorMessage(datasourceQuery.error)}</Alert>;
  }

  const handleDraftChange = (nextDraft: ConnectionDraft) => {
    setForm(current => {
      const fieldsChanged = current.draft.type !== nextDraft.type
        || JSON.stringify(current.draft.fields) !== JSON.stringify(nextDraft.fields);
      const preview = connectionPreview(nextDraft);
      const nextSecurity = nextDraft.type !== current.connectorType ? {} : { ...current.security };
      if (fieldsChanged && preview) {
        nextSecurity.connect = preview;
      }
      return {
        ...current,
        connectorType: nextDraft.type,
        draft: nextDraft,
        technicalParams: parseTechnicalParams(nextDraft.extraParams),
        security: nextSecurity,
        connectionEdited: current.connectionEdited || fieldsChanged
      };
    });
  };

  const handleEndpointValueChange = (field: EndpointField, value: string) => {
    setForm(current => ({
      ...current,
      security: { ...current.security, [securityKey(field)]: value },
      connectionEdited: field === 'connect' ? true : current.connectionEdited
    }));
  };

  const handleSecurityValueChange = (field: DatasourceSecurityField, value: string) => {
    setForm(current => ({
      ...current,
      security: { ...current.security, [datasourceSecurityKey(field)]: value }
    }));
  };

  const handleTechnicalParamChange = (key: string, value: string) => {
    setForm(current => {
      const technicalParams = { ...current.technicalParams };
      if (value.trim()) {
        technicalParams[key] = value;
      } else {
        delete technicalParams[key];
      }
      return {
        ...current,
        technicalParams,
        draft: { ...current.draft, extraParams: formatTechnicalParams(technicalParams) }
      };
    });
  };

  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextErrors: FormErrors = {};
    if (!form.name.trim()) {
      nextErrors.name = 'Name is required.';
    }
    const connect = form.security.connect?.trim();
    if (!editMode && !connect) {
      nextErrors.connect = 'A connection is required.';
    }
    if (editMode && form.connectionEdited && !connect) {
      nextErrors.connect = 'Complete the connection before saving.';
    }
    setErrors(nextErrors);
    setErrorMessageText(undefined);
    if (Object.keys(nextErrors).length > 0) {
      return;
    }
    mutation.mutate({
      name: form.name.trim(),
      connectorType: form.connectorType,
      technicalParams: form.technicalParams,
      security: form.security,
      clearSecurityKeys: form.clearSecurityKeys
    });
  };

  const values = endpointValues(form.security);
  const configured = editMode && datasourceQuery.data?.securityConfigured;

  return (
    <Stack spacing={3}>
      <PageHeader
        title={editMode ? 'Edit datasource' : 'New datasource'}
        description="Keep connection credentials in the encrypted datasource profile."
        backLink={
          <Button component={RouterLink} to="/datasources" variant="text" startIcon={<ArrowBackIcon />}>
            Back to datasources
          </Button>
        }
      />
      <Box component="form" noValidate onSubmit={submit}>
        <Stack spacing={2.5}>
          {errorMessageText && <Alert severity="error">{errorMessageText}</Alert>}
          <SurfaceSection title="Identity" description="Name the reusable profile and choose its connector.">
            <Stack spacing={2}>
              <TextField
                label="Datasource name"
                value={form.name}
                onChange={event => setForm(current => ({ ...current, name: event.target.value }))}
                error={Boolean(errors.name)}
                helperText={errors.name}
                required
                fullWidth
              />
              {configured && (
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  <Chip label="Credentials configured" color="success" size="small" />
                  <Typography variant="body2" color="text.secondary" sx={{ alignSelf: 'center' }}>
                    Existing secret values are preserved when their fields stay blank.
                  </Typography>
                </Stack>
              )}
            </Stack>
          </SurfaceSection>

          <SurfaceSection title="Connection" description="Choose a supported connector and enter its non-secret technical settings.">
            <ConnectionSettingsCard
              side="source"
              labelPrefix="Datasource"
              draft={form.draft}
              values={values}
              onDraftChange={handleDraftChange}
              onValueChange={handleEndpointValueChange}
              securityValues={securityValues(form.security)}
              onSecurityValueChange={handleSecurityValueChange}
              connectionParams={form.technicalParams}
              onConnectionParamChange={handleTechnicalParamChange}
              connectError={errors.connect}
              passwordHelperText={editMode ? 'Leave blank to keep the existing value' : undefined}
            />
          </SurfaceSection>

          <SurfaceSection title="Clear security values" description="Select values to remove explicitly. The connection itself cannot be cleared.">
            <FormGroup sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' } }}>
              {clearOptions.map(option => (
                <FormControlLabel
                  key={option.key}
                  control={
                    <Checkbox
                      checked={form.clearSecurityKeys.includes(option.key)}
                      onChange={event => setForm(current => ({
                        ...current,
                        clearSecurityKeys: event.target.checked
                          ? [...current.clearSecurityKeys, option.key]
                          : current.clearSecurityKeys.filter(key => key !== option.key)
                      }))}
                    />
                  }
                  label={option.label}
                />
              ))}
            </FormGroup>
          </SurfaceSection>

          <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
            <Button component={RouterLink} to="/datasources" variant="outlined">
              Cancel
            </Button>
            <Button type="submit" variant="contained" startIcon={<SaveIcon />} disabled={mutation.isPending}>
              {mutation.isPending ? 'Saving...' : editMode ? 'Save datasource' : 'Create datasource'}
            </Button>
          </Box>
        </Stack>
      </Box>
    </Stack>
  );
}
