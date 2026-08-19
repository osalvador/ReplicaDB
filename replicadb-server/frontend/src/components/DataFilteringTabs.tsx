import {
  Box,
  Checkbox,
  Divider,
  FormControlLabel,
  MenuItem,
  Stack,
  Tab,
  Tabs,
  TextField,
  Typography
} from '@mui/material';
import { useId, useState } from 'react';
import type { DbType } from '../utils/connectionBuilder';

export type DataFilteringValues = {
  table: string;
  columns: string;
  where: string;
  query: string;
};

type DataFilteringTabsProps = {
  values: DataFilteringValues;
  onChange: (field: keyof DataFilteringValues, value: string) => void;
  sourceType: DbType;
  fileParams: Record<string, string>;
  onFileParamChange: (key: string, value: string) => void;
  tableError?: string;
};

type FileFormatSettingsProps = {
  values: Record<string, string>;
  onChange: (key: string, value: string) => void;
  includeRecordSeparator?: boolean;
};

const formatOptions = [
  'DEFAULT',
  'ORACLE',
  'EXCEL',
  'RFC4180',
  'TDF',
  'MYSQL',
  'POSTGRESQL_CSV',
  'POSTGRESQL_TEXT',
  'MONGO_CSV',
  'MONGO_TSV',
  'INFORMIX_UNLOAD',
  'INFORMIX_UNLOAD_CSV'
];

function ToggleSetting({
  label,
  helperText,
  checked,
  onChange
}: {
  label: string;
  helperText: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <FormControlLabel
      control={<Checkbox checked={checked} onChange={event => onChange(event.target.checked)} />}
      label={<Box><Typography variant="body2">{label}</Typography><Typography variant="caption" color="text.secondary">{helperText}</Typography></Box>}
    />
  );
}

export function FileFormatSettings({ values, onChange, includeRecordSeparator = false }: FileFormatSettingsProps) {
  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1" fontWeight={700}>
        Parsing and formatting file data
      </Typography>
      <TextField
        select
        label="Format"
        value={values.format ?? ''}
        onChange={event => onChange('format', event.target.value)}
        helperText="Base predefined CSV format"
        fullWidth
      >
        <MenuItem value="">Not configured</MenuItem>
        {formatOptions.map(format => <MenuItem key={format} value={format}>{format}</MenuItem>)}
      </TextField>
      <Divider />
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr 1fr', sm: 'repeat(4, 1fr)' }, gap: 2 }}>
        <TextField
          label="Delimiter"
          value={values['format.delimiter'] ?? ''}
          onChange={event => onChange('format.delimiter', event.target.value)}
          inputProps={{ maxLength: 1 }}
          helperText="Field delimiter"
          fullWidth
        />
        <TextField
          label="Quote"
          value={values['format.quote'] ?? ''}
          onChange={event => onChange('format.quote', event.target.value)}
          inputProps={{ maxLength: 1 }}
          helperText="Quote character"
          fullWidth
        />
        <TextField
          label="Escape"
          value={values['format.escape'] ?? ''}
          onChange={event => onChange('format.escape', event.target.value)}
          inputProps={{ maxLength: 1 }}
          helperText="Escape character"
          fullWidth
        />
        {includeRecordSeparator && (
          <TextField
            label="Record separator"
            value={values['format.recordSeparator'] ?? ''}
            onChange={event => onChange('format.recordSeparator', event.target.value)}
            helperText="Record delimiter"
            fullWidth
          />
        )}
      </Box>
      <TextField
        label="Null string"
        value={values['format.nullString'] ?? ''}
        onChange={event => onChange('format.nullString', event.target.value)}
        helperText="String converted to null"
        fullWidth
      />
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 1 }}>
        <ToggleSetting
          label="First record as header"
          helperText="The first line is the header"
          checked={values['format.firstRecordAsHeader'] === 'true'}
          onChange={checked => onChange('format.firstRecordAsHeader', String(checked))}
        />
        <ToggleSetting
          label="Ignore empty lines"
          helperText="Empty lines between records are ignored"
          checked={values['format.ignoreEmptyLines'] === 'true'}
          onChange={checked => onChange('format.ignoreEmptyLines', String(checked))}
        />
        <ToggleSetting
          label="Ignore surrounding spaces"
          helperText="Spaces around values are ignored"
          checked={values['format.ignoreSurroundingSpaces'] === 'true'}
          onChange={checked => onChange('format.ignoreSurroundingSpaces', String(checked))}
        />
        <ToggleSetting
          label="Trim"
          helperText="Trim leading and trailing blanks"
          checked={values['format.trim'] === 'true'}
          onChange={checked => onChange('format.trim', String(checked))}
        />
      </Box>
    </Stack>
  );
}

export default function DataFilteringTabs({
  values,
  onChange,
  sourceType,
  fileParams,
  onFileParamChange,
  tableError
}: DataFilteringTabsProps) {
  const [tab, setTab] = useState<'options' | 'query'>(values.query ? 'query' : 'options');
  const tabId = useId();

  const changeTab = (nextTab: 'options' | 'query') => {
    setTab(nextTab);
  };

  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1" fontWeight={700}>
        Data filtering
      </Typography>
      <Tabs
        value={tab}
        onChange={(_, value: 'options' | 'query') => changeTab(value)}
        aria-label="Data filtering mode"
      >
        <Tab
          id={`${tabId}-options-tab`}
          aria-controls={`${tabId}-options-panel`}
          label="Options"
          value="options"
        />
        <Tab
          id={`${tabId}-query-tab`}
          aria-controls={`${tabId}-query-panel`}
          label="Query"
          value="query"
        />
      </Tabs>
      {tab === 'options' ? (
        <Box role="tabpanel" id={`${tabId}-options-panel`} aria-labelledby={`${tabId}-options-tab`} sx={{ pt: 1 }}>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' }, gap: 2 }}>
          <TextField
            label="Table"
            value={values.table}
            onChange={event => onChange('table', event.target.value)}
            error={Boolean(tableError)}
            helperText={tableError ?? 'Table to read, for example public.employees'}
            fullWidth
          />
          <TextField
            label="Columns"
            value={values.columns}
            onChange={event => onChange('columns', event.target.value)}
            helperText="Comma-delimited columns to replicate"
            fullWidth
          />
          <TextField
            label="Where"
            value={values.where}
            onChange={event => onChange('where', event.target.value)}
            helperText="Optional WHERE clause"
            fullWidth
          />
          </Box>
        </Box>
      ) : (
        <Box role="tabpanel" id={`${tabId}-query-panel`} aria-labelledby={`${tabId}-query-tab`} sx={{ pt: 1 }}>
          <TextField
            label="Query"
            value={values.query}
            onChange={event => onChange('query', event.target.value)}
            helperText="SQL statement executed in the source database"
            multiline
            minRows={6}
            fullWidth
          />
        </Box>
      )}
      {sourceType === 'file' && (
        <Box role="group" aria-label="File format settings" sx={{ borderTop: 1, borderColor: 'divider', pt: 2 }}>
          <FileFormatSettings values={fileParams} onChange={onFileParamChange} />
        </Box>
      )}
    </Stack>
  );
}
