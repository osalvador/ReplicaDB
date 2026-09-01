import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import {
  Alert,
  Autocomplete,
  Button,
  Checkbox,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { Link as RouterLink, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import {
  datasourceQueryKeys,
  getDatasource,
  listDatasourcePermissions,
  replaceDatasourcePermission,
  revokeDatasourcePermission,
  type DatasourcePermissionResponse
} from '../api/datasourcesApi';
import { listUsers, type UserResponse } from '../api/usersApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

const permissionTypes = ['VIEW', 'USE', 'EDIT'] as const;
type PermissionType = typeof permissionTypes[number];

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof ApiError ? error.detail : fallback;
}

function permissionsForGrant(grant: DatasourcePermissionResponse): PermissionType[] {
  const permissions = grant.permissions ?? [];
  return permissionTypes.filter(permission => permissions.includes(permission));
}

export default function DatasourcePermissionsPage() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const [grantOpen, setGrantOpen] = useState(false);
  const [selectedUser, setSelectedUser] = useState<UserResponse | null>(null);
  const [selectedPermissions, setSelectedPermissions] = useState<PermissionType[]>([]);
  const [rowPermissions, setRowPermissions] = useState<Record<string, PermissionType[]>>({});
  const [permissionError, setPermissionError] = useState<string>();

  const datasourceQuery = useQuery({
    queryKey: datasourceQueryKeys.detail(id ?? ''),
    queryFn: () => getDatasource(id ?? ''),
    enabled: Boolean(id)
  });
  const permissionsQuery = useQuery({
    queryKey: datasourceQueryKeys.permissions(id ?? ''),
    queryFn: () => listDatasourcePermissions(id ?? ''),
    enabled: Boolean(id)
  });
  const usersQuery = useQuery({
    queryKey: ['users', 'all'],
    queryFn: () => listUsers(0, 200),
    enabled: grantOpen
  });

  const permissionMutation = useMutation({
    mutationFn: ({ userId, permissions }: { userId: string; permissions: PermissionType[] }) =>
      replaceDatasourcePermission(id ?? '', userId, { permissions }),
    onSuccess: (_response, variables) => {
      void queryClient.invalidateQueries({ queryKey: datasourceQueryKeys.permissions(id ?? '') });
      setRowPermissions(current => {
        const next = { ...current };
        delete next[variables.userId];
        return next;
      });
      closeGrantDialog();
    },
    onError: error => setPermissionError(errorMessage(error, 'Unable to update datasource permissions.'))
  });

  const revokeMutation = useMutation({
    mutationFn: (userId: string) => revokeDatasourcePermission(id ?? '', userId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: datasourceQueryKeys.permissions(id ?? '') });
    },
    onError: error => setPermissionError(errorMessage(error, 'Unable to remove datasource permissions.'))
  });

  const closeGrantDialog = () => {
    setGrantOpen(false);
    setSelectedUser(null);
    setSelectedPermissions([]);
    setPermissionError(undefined);
    permissionMutation.reset();
  };

  const openGrantDialog = () => {
    setSelectedUser(null);
    setSelectedPermissions([]);
    setPermissionError(undefined);
    permissionMutation.reset();
    setGrantOpen(true);
  };

  if (datasourceQuery.isPending || permissionsQuery.isPending) {
    return <LoadingState label="Loading datasource permissions" />;
  }

  if (datasourceQuery.isError || !datasourceQuery.data) {
    return <Alert severity="error">{errorMessage(datasourceQuery.error, 'Unable to load this datasource.')}</Alert>;
  }

  if (permissionsQuery.isError) {
    return <Alert severity="error">{errorMessage(permissionsQuery.error, 'Unable to load datasource permissions.')}</Alert>;
  }

  const datasource = datasourceQuery.data;
  const grants = permissionsQuery.data ?? [];
  const existingUserIds = new Set(grants.map(grant => grant.userId).filter((userId): userId is string => Boolean(userId)));
  const availableUsers = (usersQuery.data?.content ?? []).filter(user => user.id && !existingUserIds.has(user.id));
  const rowPermissionsFor = (grant: DatasourcePermissionResponse) =>
    rowPermissions[grant.userId ?? ''] ?? permissionsForGrant(grant);

  const toggleRowPermission = (grant: DatasourcePermissionResponse, permission: PermissionType) => {
    if (!grant.userId) {
      return;
    }
    const current = rowPermissionsFor(grant);
    const next = current.includes(permission)
      ? current.filter(value => value !== permission)
      : [...current, permission];
    setRowPermissions(currentRows => ({
      ...currentRows,
      [grant.userId as string]: permissionTypes.filter(value => next.includes(value))
    }));
  };

  const submitGrant = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedUser?.id) {
      setPermissionError('Select a user before granting access.');
      return;
    }
    if (selectedPermissions.length === 0) {
      setPermissionError('Select at least one permission.');
      return;
    }
    setPermissionError(undefined);
    permissionMutation.mutate({ userId: selectedUser.id, permissions: selectedPermissions });
  };

  return (
    <Stack spacing={3}>
      <PageHeader
        title={`${datasource.name ?? 'Datasource'} permissions`}
        description="Manage which users can inspect and bind this datasource."
        backLink={
          <Button component={RouterLink} to="/datasources" variant="text" startIcon={<ArrowBackIcon />}>
            Back to datasources
          </Button>
        }
      />
      <SurfaceSection title="Datasource profile">
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} alignItems={{ xs: 'flex-start', sm: 'center' }}>
          <Typography fontWeight={700}>{datasource.safeConnectDisplay ?? 'Connection not configured'}</Typography>
          <Typography variant="body2" color="text.secondary">{datasource.connectorType}</Typography>
        </Stack>
      </SurfaceSection>
      <SurfaceSection
        title="Access control"
        description="VIEW exposes safe metadata, USE permits job binding, and EDIT permits profile changes."
        actions={<Button variant="contained" onClick={openGrantDialog}>Grant access</Button>}
      >
        {grants.length === 0 ? (
          <EmptyState title="No users have explicit access to this datasource." />
        ) : (
          <TableContainer sx={{ overflowX: 'auto' }}>
            <Table aria-label="Datasource permissions" sx={{ minWidth: 720 }}>
              <TableHead>
                <TableRow>
                  <TableCell>Username</TableCell>
                  {permissionTypes.map(permission => <TableCell key={permission} align="center">{permission}</TableCell>)}
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {grants.map(grant => (
                  <TableRow key={grant.userId ?? grant.username}>
                    <TableCell>{grant.username ?? 'Unknown'}</TableCell>
                    {permissionTypes.map(permission => (
                      <TableCell key={permission} align="center">
                        <Checkbox
                          checked={rowPermissionsFor(grant).includes(permission)}
                          onChange={() => toggleRowPermission(grant, permission)}
                          inputProps={{ 'aria-label': `${permission} permission for ${grant.username ?? 'unknown user'}` }}
                        />
                      </TableCell>
                    ))}
                    <TableCell align="right">
                      <Button
                        size="small"
                        onClick={() => grant.userId && permissionMutation.mutate({
                          userId: grant.userId,
                          permissions: rowPermissionsFor(grant)
                        })}
                        disabled={permissionMutation.isPending || !grant.userId}
                        aria-label={`Save permissions for ${grant.username ?? 'unknown user'}`}
                      >
                        Save
                      </Button>
                      <Button
                        color="error"
                        size="small"
                        onClick={() => grant.userId && revokeMutation.mutate(grant.userId)}
                        disabled={revokeMutation.isPending || !grant.userId}
                        aria-label={`Remove permissions for ${grant.username ?? 'unknown user'}`}
                      >
                        Remove
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
        {permissionError && !grantOpen && <Alert severity="error" sx={{ mt: 2 }}>{permissionError}</Alert>}
      </SurfaceSection>
      <Dialog
        open={grantOpen}
        onClose={() => !permissionMutation.isPending && closeGrantDialog()}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Grant datasource access</DialogTitle>
        <DialogContent>
          <Stack component="form" id="grant-datasource-permission-form" noValidate onSubmit={submitGrant} spacing={2} sx={{ pt: 1 }}>
            {permissionError && <Alert severity="error">{permissionError}</Alert>}
            {usersQuery.isError && <Alert severity="error">{errorMessage(usersQuery.error, 'Unable to load users.')}</Alert>}
            <Autocomplete
              options={availableUsers}
              value={selectedUser}
              onChange={(_, user) => setSelectedUser(user)}
              getOptionLabel={user => user.username ?? ''}
              isOptionEqualToValue={(option, value) => option.id === value.id}
              loading={usersQuery.isPending}
              disabled={usersQuery.isError}
              renderInput={params => <TextField {...params} label="User" required />}
              fullWidth
              autoHighlight
            />
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
              {permissionTypes.map(permission => (
                <FormControlLabel
                  key={permission}
                  label={permission}
                  control={
                    <Checkbox
                      checked={selectedPermissions.includes(permission)}
                      onChange={event => setSelectedPermissions(current => event.target.checked
                        ? [...current, permission]
                        : current.filter(value => value !== permission))}
                    />
                  }
                />
              ))}
            </Stack>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeGrantDialog}>Cancel</Button>
          <Button type="submit" form="grant-datasource-permission-form" variant="contained" disabled={permissionMutation.isPending}>
            {permissionMutation.isPending ? 'Saving...' : 'Grant'}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
