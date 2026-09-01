import {
  Alert,
  Autocomplete,
  CircularProgress,
  ListItemText,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { ApiError } from '../api/client';
import {
  datasourceQueryKeys,
  listDatasources,
  type DatasourceResponse,
  type DatasourceRole
} from '../api/datasourcesApi';

export type DatasourceSummary = {
  id?: string;
  name?: string;
  connectorType?: string;
  safeConnectDisplay?: string;
};

type DatasourceOption = {
  id: string;
  name?: string;
  connectorType?: string;
  safeConnectDisplay?: string;
  canUse: boolean;
};

type DatasourceSelectorProps = {
  side: DatasourceRole;
  value: string;
  selectedSummary?: DatasourceSummary | null;
  onChange: (datasourceId: string) => void;
  disabled?: boolean;
  error?: string;
};

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : `Unable to load ${errorRole(error)} datasources.`;
}

function errorRole(error: unknown): string {
  return error instanceof Error && error.message ? error.message : 'available';
}

function toOption(datasource: DatasourceResponse | DatasourceSummary, canUse: boolean): DatasourceOption | null {
  if (!datasource.id) {
    return null;
  }
  return {
    id: datasource.id,
    name: datasource.name,
    connectorType: datasource.connectorType,
    safeConnectDisplay: datasource.safeConnectDisplay,
    canUse
  };
}

function optionLabel(option: DatasourceOption): string {
  const name = option.name ?? 'Unnamed datasource';
  return option.connectorType ? `${name} (${option.connectorType})` : name;
}

export default function DatasourceSelector({
  side,
  value,
  selectedSummary,
  onChange,
  disabled = false,
  error
}: DatasourceSelectorProps) {
  const query = useQuery({
    queryKey: datasourceQueryKeys.list(0, 200, side),
    queryFn: () => listDatasources(0, 200, side)
  });

  const usableOptions = (query.data?.content ?? [])
    .map(datasource => toOption(datasource, datasource.canUse === true))
    .filter((option): option is DatasourceOption => Boolean(option && option.canUse));
  const usableIds = new Set(usableOptions.map(option => option.id));
  const selectedOption = selectedSummary && selectedSummary.id
    ? toOption(selectedSummary, usableIds.has(selectedSummary.id))
    : null;
  const options = selectedOption && !usableIds.has(selectedOption.id)
    ? [...usableOptions, selectedOption]
    : usableOptions;
  const valueOption = options.find(option => option.id === value) ?? null;
  const label = `${side === 'source' ? 'Source' : 'Sink'} datasource`;
  const roleLabel = side === 'source' ? 'source-capable' : 'sink-capable';
  const helperText = error
    ?? (query.isError
      ? errorMessage(query.error)
      : valueOption && !usableIds.has(valueOption.id)
        ? 'Current binding is unavailable for USE; enable it only after access is restored.'
        : `Choose a ${roleLabel} datasource you can use.`);

  return (
    <Stack spacing={1}>
      <Autocomplete
        options={options}
        value={valueOption}
        onChange={(_, option) => onChange(option?.id ?? '')}
        getOptionLabel={optionLabel}
        isOptionEqualToValue={(option, selected) => option.id === selected.id}
        getOptionDisabled={option => option.id === value && !usableIds.has(option.id)}
        loading={query.isPending}
        disabled={disabled || query.isError}
        noOptionsText={`No ${roleLabel} datasources available with USE access`}
        loadingText="Loading datasources..."
        renderOption={(props, option) => (
          <li {...props} key={option.id}>
            <ListItemText
              primary={option.name ?? 'Unnamed datasource'}
              secondary={option.connectorType ?? 'Unknown connector'}
            />
          </li>
        )}
        renderInput={params => (
          <TextField
            {...params}
            label={label}
            required
            error={Boolean(error) || query.isError}
            helperText={helperText}
            InputProps={{
              ...params.InputProps,
              endAdornment: (
                <>
                  {query.isPending ? <CircularProgress color="inherit" size={18} /> : null}
                  {params.InputProps.endAdornment}
                </>
              )
            }}
          />
        )}
      />
      {valueOption && !usableIds.has(valueOption.id) && (
        <Alert severity="warning">
          <Typography variant="body2">
            This binding cannot be re-enabled until you have USE access to the selected datasource.
          </Typography>
        </Alert>
      )}
    </Stack>
  );
}
