import AddIcon from '@mui/icons-material/Add';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import SecurityOutlinedIcon from '@mui/icons-material/SecurityOutlined';
import {
  Alert,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
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
  Tooltip,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { ApiError } from '../api/client';
import {
  datasourceQueryKeys,
  deleteDatasource,
  invalidateDatasourceQueries,
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
  const queryClient = useQueryClient();
  const [page, setPage] = useState(0);
  const [role, setRole] = useState<DatasourceRole | ''>('');
  const [deleteTarget, setDeleteTarget] = useState<DatasourceResponse | null>(null);

  const size = 25;
  const datasourcesQuery = useQuery({
    queryKey: datasourceQueryKeys.list(page, size, role || undefined),
    queryFn: () => listDatasources(page, size, role || undefined)
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => deleteDatasource(id),
    onSuccess: () => {
      void invalidateDatasourceQueries(queryClient);
      setDeleteTarget(null);
    }
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
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {datasources.map(datasource => {
                    const capabilities = capabilityLabels(datasource);
                    return (
                      <TableRow key={datasource.id ?? datasource.name}>
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
                        <TableCell align="right">
                          <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                            {datasource.canEdit && datasource.id && (
                              <Button
                                component={RouterLink}
                                to={`/datasources/${datasource.id}/edit`}
                                size="small"
                                startIcon={<EditOutlinedIcon />}
                                aria-label={`Edit ${datasource.name ?? 'datasource'}`}
                              >
                                Edit
                              </Button>
                            )}
                            {user?.role === 'ADMIN' && datasource.id && (
                              <Tooltip title="Manage datasource permissions">
                                <Button
                                  component={RouterLink}
                                  to={`/datasources/${datasource.id}/permissions`}
                                  size="small"
                                  startIcon={<SecurityOutlinedIcon />}
                                  aria-label={`Permissions for ${datasource.name ?? 'datasource'}`}
                                >
                                  ACL
                                </Button>
                              </Tooltip>
                            )}
                            {user?.role === 'ADMIN' && datasource.id && (
                              <Button
                                color="error"
                                size="small"
                                startIcon={<DeleteOutlineIcon />}
                                onClick={() => setDeleteTarget(datasource)}
                                aria-label={`Delete ${datasource.name ?? 'datasource'}`}
                              >
                                Delete
                              </Button>
                            )}
                          </Stack>
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
        {deleteMutation.isError && (
          <Alert severity="error" sx={{ mt: 2 }}>{errorMessage(deleteMutation.error)}</Alert>
        )}
      </SurfaceSection>

      <Dialog
        open={Boolean(deleteTarget)}
        onClose={() => !deleteMutation.isPending && setDeleteTarget(null)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Delete datasource</DialogTitle>
        <DialogContent>
          <Typography>
            Delete {deleteTarget?.name ?? 'this datasource'}? A profile referenced by a job cannot be deleted.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteTarget(null)} disabled={deleteMutation.isPending}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={() => deleteTarget?.id && deleteMutation.mutate(deleteTarget.id)}
            disabled={deleteMutation.isPending || !deleteTarget?.id}
            startIcon={<DeleteOutlineIcon />}
          >
            {deleteMutation.isPending ? 'Deleting...' : 'Delete datasource'}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
