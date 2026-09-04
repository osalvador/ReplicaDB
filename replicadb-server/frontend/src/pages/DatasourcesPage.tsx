import AddIcon from '@mui/icons-material/Add';
import {
  Alert,
  Button,
  Chip,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Typography
} from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import { ApiError } from '../api/client';
import {
  datasourceQueryKeys,
  listDatasources,
  type DatasourceResponse,
  type DatasourceRole
} from '../api/datasourcesApi';
import { useAuth } from '../auth/useAuth';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to load datasources.';
}

function capabilityLabels(datasource: DatasourceResponse): string[] {
  const capabilities = datasource.capabilities;
  return [
    capabilities?.sourceCapable ? 'Source' : undefined,
    capabilities?.sinkCapable ? 'Sink' : undefined
  ].filter((label): label is string => Boolean(label));
}

export default function DatasourcesPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [page, setPage] = useState(0);
  const [role, setRole] = useState<DatasourceRole | ''>('');

  const size = 25;
  const datasourcesQuery = useQuery({
    queryKey: datasourceQueryKeys.list(page, size, role || undefined),
    queryFn: () => listDatasources(page, size, role || undefined)
  });

  if (datasourcesQuery.isPending) {
    return <LoadingState label="Loading datasources" />;
  }

  if (datasourcesQuery.isError) {
    return <Alert severity="error">{errorMessage(datasourcesQuery.error)}</Alert>;
  }

  const datasources = datasourcesQuery.data.content ?? [];
  const totalElements = datasourcesQuery.data.totalElements ?? 0;
  const paginationCount = datasources.length < size ? page * size + datasources.length : totalElements;

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Datasources"
        description="Reusable connection profiles for managed replication jobs."
        actions={user?.role === 'ADMIN' ? (
          <Button component={RouterLink} to="/datasources/new" variant="contained" startIcon={<AddIcon />}>
            New datasource
          </Button>
        ) : undefined}
      />
      <SurfaceSection
        title="Datasource catalog"
        description="Only redacted connection details and non-secret technical parameters are shown."
        actions={
          <TextField
            select
            label="Role filter"
            value={role}
            onChange={event => {
              setRole(event.target.value as DatasourceRole | '');
              setPage(0);
            }}
            sx={{ minWidth: 150 }}
          >
            <MenuItem value="">All roles</MenuItem>
            <MenuItem value="source">Source capable</MenuItem>
            <MenuItem value="sink">Sink capable</MenuItem>
          </TextField>
        }
      >
        {datasources.length === 0 ? (
          <EmptyState
            title="No datasources configured."
            description={user?.role === 'ADMIN' ? 'Create a profile before defining a job.' : 'Ask an administrator for access.'}
          />
        ) : (
          <>
            <TableContainer sx={{ overflowX: 'auto' }}>
              <Table aria-label="Datasources" sx={{ minWidth: 920 }}>
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Connector</TableCell>
                    <TableCell>Safe connection</TableCell>
                    <TableCell>Capabilities</TableCell>
                    <TableCell>Security</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {datasources.map(datasource => {
                    const capabilities = capabilityLabels(datasource);
                    const openDatasource = () => {
                      if (datasource.id) {
                        navigate(`/datasources/${datasource.id}`);
                      }
                    };
                    return (
                      <TableRow
                        key={datasource.id ?? datasource.name}
                        hover={Boolean(datasource.id)}
                        tabIndex={datasource.id ? 0 : undefined}
                        aria-label={datasource.id ? `Open ${datasource.name ?? 'datasource'}` : undefined}
                        onClick={openDatasource}
                        onKeyDown={event => {
                          if (datasource.id && (event.key === 'Enter' || event.key === ' ')) {
                            event.preventDefault();
                            openDatasource();
                          }
                        }}
                        sx={{ cursor: datasource.id ? 'pointer' : 'default' }}
                      >
                        <TableCell>
                          {datasource.id ? (
                            <Typography
                              component={RouterLink}
                              to={`/datasources/${datasource.id}`}
                              fontWeight={700}
                              sx={{ color: 'primary.main', textDecoration: 'none' }}
                            >
                              {datasource.name ?? 'Unnamed datasource'}
                            </Typography>
                          ) : (
                            <Typography fontWeight={700}>{datasource.name ?? 'Unnamed datasource'}</Typography>
                          )}
                          <Typography variant="body2" color="text.secondary">
                            {datasource.technicalParams && Object.keys(datasource.technicalParams).length > 0
                              ? `${Object.keys(datasource.technicalParams).length} technical setting(s)`
                              : 'No technical settings'}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip label={datasource.connectorType ?? 'Unknown'} size="small" variant="outlined" />
                        </TableCell>
                        <TableCell sx={{ maxWidth: 280, overflowWrap: 'anywhere' }}>
                          {datasource.safeConnectDisplay ?? 'Not configured'}
                        </TableCell>
                        <TableCell>
                          <Stack direction="row" spacing={0.75} flexWrap="wrap" useFlexGap>
                            {capabilities.length > 0
                              ? capabilities.map(label => <Chip key={label} label={label} size="small" />)
                              : <Typography variant="body2" color="text.secondary">Unavailable</Typography>}
                          </Stack>
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={datasource.securityConfigured ? 'Configured' : 'Not configured'}
                            size="small"
                            color={datasource.securityConfigured ? 'success' : 'default'}
                            variant={datasource.securityConfigured ? 'filled' : 'outlined'}
                          />
                        </TableCell>
                      </TableRow>
                    );
                  })}
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
          </>
        )}
      </SurfaceSection>
    </Stack>
  );
}
