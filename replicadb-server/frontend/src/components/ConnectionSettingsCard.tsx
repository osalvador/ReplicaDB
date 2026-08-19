import {
  Box,
  Button,
  Collapse,
  MenuItem,
  Radio,
  RadioGroup,
  Stack,
  TextField,
  FormControlLabel,
  FormLabel,
  Typography
} from '@mui/material';
import { ExpandMore } from '@mui/icons-material';
import { useId, useState } from 'react';
import {
  buildConnectString,
  type ConnectionFields,
  type DbType,
  type OracleFormat
} from '../utils/connectionBuilder';

export type ConnectionDraft = {
  type: DbType;
  fields: ConnectionFields;
  extraParams: string;
};

export type EndpointField =
  | 'connect'
  | 'user'
  | 'password'
  | 'authMode'
  | 'authPrincipalId'
  | 'authLoginHint'
  | 'authClientCertificate'
  | 'authClientKey';

export type EndpointValues = Record<EndpointField, string>;

type ConnectionSettingsCardProps = {
  side: 'source' | 'sink';
  draft: ConnectionDraft;
  values: EndpointValues;
  onDraftChange: (draft: ConnectionDraft) => void;
  onValueChange: (field: EndpointField, value: string) => void;
  connectionParams?: Record<string, string>;
  onConnectionParamChange?: (key: string, value: string) => void;
  connectError?: string;
  passwordHelperText?: string;
};

const databaseTypes: Array<{ value: Exclude<DbType, 'custom' | 'kafka'>; label: string }> = [
  { value: 'oracle', label: 'Oracle' },
  { value: 'mysql', label: 'MySQL' },
  { value: 'mariadb', label: 'MariaDB' },
  { value: 'postgres', label: 'PostgreSQL' },
  { value: 'db2', label: 'DB2 LUW' },
  { value: 'db2i', label: 'DB2 for i' },
  { value: 'sqlite', label: 'SQLite' },
  { value: 'sqlserver', label: 'SQL Server' },
  { value: 'denodo', label: 'Denodo' },
  { value: 'file', label: 'File' }
];

const authModes = [
  ['ActiveDirectoryInteractive', 'Interactive (browser and MFA)'],
  ['ActiveDirectoryDefault', 'Default credential / Azure CLI'],
  ['ActiveDirectoryManagedIdentity', 'Managed identity'],
  ['ActiveDirectoryServicePrincipal', 'Service principal secret'],
  ['ActiveDirectoryServicePrincipalCertificate', 'Service principal certificate'],
  ['ActiveDirectoryIntegrated', 'Integrated / Kerberos']
] as const;

function displayType(type: DbType): string {
  if (type === 'custom') {
    return 'Custom connection string';
  }
  if (type === 'kafka') {
    return 'Apache Kafka';
  }
  return databaseTypes.find(option => option.value === type)?.label ?? type;
}

function previewConnection(draft: ConnectionDraft): string {
  try {
    return buildConnectString(draft.type, draft.fields);
  } catch {
    return draft.type === 'custom' ? draft.fields.raw ?? '' : '';
  }
}

function updateField(draft: ConnectionDraft, field: keyof ConnectionFields, value: string): ConnectionDraft {
  return { ...draft, fields: { ...draft.fields, [field]: value } };
}

export default function ConnectionSettingsCard({
  side,
  draft,
  values,
  onDraftChange,
  onValueChange,
  connectionParams = {},
  onConnectionParamChange = () => undefined,
  connectError,
  passwordHelperText
}: ConnectionSettingsCardProps) {
  const [authOpen, setAuthOpen] = useState(Boolean(values.authMode));
  const isKafka = draft.type === 'kafka';
  const isFile = draft.type === 'file';
  const isSqlite = draft.type === 'sqlite';
  const isCustom = draft.type === 'custom';
  const isSqlServer = draft.type === 'sqlserver';
  const isOracle = draft.type === 'oracle';
  const authPanelId = useId();
  const options = side === 'sink'
    ? [...databaseTypes, { value: 'kafka' as const, label: 'Apache Kafka' }]
    : databaseTypes;
  const preview = previewConnection(draft);

  return (
    <Stack spacing={2}>
      <Box>
        <Typography component="h3" variant="subtitle1">
          {side === 'source' ? 'Source connection' : 'Sink connection'}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Select a connector and provide its connection details.
        </Typography>
      </Box>
      <TextField
        select
        label={`${side === 'source' ? 'Source' : 'Sink'} data source type`}
        value={draft.type}
        onChange={event => onDraftChange({
          ...draft,
          type: event.target.value as DbType,
          fields: event.target.value === 'custom' ? { raw: values.connect } : draft.fields
        })}
        fullWidth
      >
        <MenuItem value="custom">{displayType('custom')}</MenuItem>
        {options.map(option => <MenuItem key={option.value} value={option.value}>{option.label}</MenuItem>)}
      </TextField>

      {isCustom && (
        <TextField
          label={`${side === 'source' ? 'Source' : 'Sink'} connection`}
          value={draft.fields.raw ?? values.connect}
          onChange={event => {
            onDraftChange(updateField(draft, 'raw', event.target.value));
            onValueChange('connect', event.target.value);
          }}
          error={Boolean(connectError)}
          helperText={connectError}
          required
          fullWidth
        />
      )}

      {!isCustom && !isSqlite && !isFile && !isKafka && (
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'minmax(0, 3fr) minmax(120px, 1fr)' }, gap: 2 }}>
          <TextField
            label="Host"
            value={draft.fields.host ?? ''}
            onChange={event => onDraftChange(updateField(draft, 'host', event.target.value))}
            fullWidth
          />
          <TextField
            label="Port"
            type="number"
            value={draft.fields.port ?? ''}
            onChange={event => onDraftChange(updateField(draft, 'port', event.target.value))}
            fullWidth
          />
        </Box>
      )}

      {!isCustom && !isSqlite && !isFile && !isKafka && (
        <TextField
          label="Database / SID or Service Name"
          value={draft.fields.database ?? ''}
          onChange={event => onDraftChange(updateField(draft, 'database', event.target.value))}
          fullWidth
        />
      )}

      {isOracle && (
        <Box>
          <FormLabel>Oracle connection format</FormLabel>
          <RadioGroup
            row
            value={draft.fields.oracleFormat ?? 'service'}
            onChange={event => onDraftChange(updateField(draft, 'oracleFormat', event.target.value as OracleFormat))}
          >
            <FormControlLabel value="service" control={<Radio />} label="Service name" />
            <FormControlLabel value="sid" control={<Radio />} label="SID" />
          </RadioGroup>
        </Box>
      )}

      {isSqlite && (
        <TextField
          label="Database file path"
          value={draft.fields.sqliteFilePath ?? ''}
          onChange={event => onDraftChange(updateField(draft, 'sqliteFilePath', event.target.value))}
          fullWidth
        />
      )}

      {isFile && (
        <TextField
          label="File path"
          value={draft.fields.filePath ?? ''}
          onChange={event => onDraftChange(updateField(draft, 'filePath', event.target.value))}
          fullWidth
        />
      )}

      {isKafka && (
        <Stack spacing={2}>
          <TextField
            label="Bootstrap servers"
            value={draft.fields.kafkaBootstrapServers ?? ''}
            onChange={event => onDraftChange(updateField(draft, 'kafkaBootstrapServers', event.target.value))}
            helperText="Comma-separated broker host and port pairs"
            fullWidth
          />
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '2fr 1fr 1fr' }, gap: 2 }}>
            <TextField
              label="Topic name"
              value={connectionParams.topic ?? ''}
              onChange={event => onConnectionParamChange('topic', event.target.value)}
              fullWidth
            />
            <TextField
              label="Topic partition"
              type="number"
              value={connectionParams.partition ?? ''}
              onChange={event => onConnectionParamChange('partition', event.target.value)}
              fullWidth
            />
            <TextField
              select
              label="ACKs"
              value={connectionParams.acks ?? ''}
              onChange={event => onConnectionParamChange('acks', event.target.value)}
              fullWidth
            >
              <MenuItem value="">Not configured</MenuItem>
              <MenuItem value="all">all</MenuItem>
              <MenuItem value="0">0</MenuItem>
              <MenuItem value="1">1</MenuItem>
            </TextField>
          </Box>
        </Stack>
      )}

      {!isCustom && (
        <TextField
          label={`${side === 'source' ? 'Source' : 'Sink'} connection`}
          value={preview}
          inputProps={{ readOnly: true, 'aria-readonly': 'true' }}
          error={Boolean(connectError)}
          helperText={connectError ?? 'Generated from the connection settings above'}
          fullWidth
        />
      )}

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
        <TextField
          label={`${side === 'source' ? 'Source' : 'Sink'} user`}
          value={values.user}
          onChange={event => onValueChange('user', event.target.value)}
          fullWidth
        />
        <TextField
          label={`${side === 'source' ? 'Source' : 'Sink'} password`}
          type="password"
          value={values.password}
          onChange={event => onValueChange('password', event.target.value)}
          helperText={passwordHelperText}
          fullWidth
        />
      </Box>

      {isSqlServer && (
        <Box sx={{ borderTop: 1, borderColor: 'divider', pt: 1 }}>
          <Button
            id={`${side}-entra-authentication-toggle`}
            size="small"
            color="inherit"
            aria-expanded={authOpen}
            aria-controls={authPanelId}
            endIcon={<ExpandMore sx={{ transform: authOpen ? 'rotate(180deg)' : 'none', transition: 'transform 160ms ease' }} />}
            onClick={() => setAuthOpen(open => !open)}
          >
            Microsoft Entra Authentication
          </Button>
          <Collapse in={authOpen} id={authPanelId} role="region" aria-labelledby={`${side}-entra-authentication-toggle`}>
            <Stack spacing={2} sx={{ pt: 2 }}>
              <TextField
                select
                label="Authentication mode"
                value={values.authMode}
                onChange={event => onValueChange('authMode', event.target.value)}
                fullWidth
              >
                <MenuItem value="">Not configured</MenuItem>
                {authModes.map(([value, label]) => <MenuItem key={value} value={value}>{label}</MenuItem>)}
              </TextField>
              <TextField
                label="Principal or managed identity client ID"
                value={values.authPrincipalId}
                onChange={event => onValueChange('authPrincipalId', event.target.value)}
                fullWidth
              />
              <TextField
                label="Interactive login hint"
                value={values.authLoginHint}
                onChange={event => onValueChange('authLoginHint', event.target.value)}
                fullWidth
              />
              <TextField
                label="Client certificate path"
                value={values.authClientCertificate}
                onChange={event => onValueChange('authClientCertificate', event.target.value)}
                fullWidth
              />
              <TextField
                label="Client private key path"
                value={values.authClientKey}
                onChange={event => onValueChange('authClientKey', event.target.value)}
                fullWidth
              />
            </Stack>
          </Collapse>
        </Box>
      )}

      <TextField
        label={isKafka ? 'Extra Kafka producer properties' : 'Extra JDBC parameters'}
        value={draft.extraParams}
        onChange={event => onDraftChange({ ...draft, extraParams: event.target.value })}
        multiline
        minRows={2}
        helperText="One key=value pair per line"
        fullWidth
      />
    </Stack>
  );
}
