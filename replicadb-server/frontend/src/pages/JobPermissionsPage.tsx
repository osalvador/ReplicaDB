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
  TextField
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { useParams, Link as RouterLink } from 'react-router-dom';
import { ApiError } from '../api/client';
import { getJob } from '../api/jobsApi';
import {
  deleteJobPermission,
  listJobPermissions,
  replaceJobPermission,
  type JobPermissionResponse,
  type JobPermissionRequest
} from '../api/jobPermissionsApi';
import { listUsers, type UserResponse } from '../api/usersApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

const permissionTypes = ['VIEW', 'EDIT', 'EXECUTE', 'CANCEL'] as const;
type PermissionType = typeof permissionTypes[number];

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof ApiError ? error.detail : fallback;
}

function hasPermission(grant: JobPermissionResponse, permission: PermissionType): boolean {
  return (grant.permissions ?? []).includes(permission);
}

function permissionsForGrant(grant: JobPermissionResponse): PermissionType[] {
  return permissionTypes.filter(permission => hasPermission(grant, permission));
}

export default function JobPermissionsPage() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const [grantOpen, setGrantOpen] = useState(false);
  const [selectedUser, setSelectedUser] = useState<UserResponse | null>(null);
  const [selectedPermissions, setSelectedPermissions] = useState<PermissionType[]>([]);
  const [rowPermissions, setRowPermissions] = useState<Record<string, PermissionType[]>>({});
  const [permissionError, setPermissionError] = useState<string>();

  const jobQuery = useQuery({
    queryKey: ['jobs', id],
    queryFn: () => getJob(id ?? ''),
    enabled: Boolean(id)
  });
  const permissionsQuery = useQuery({
    queryKey: ['jobPermissions', id],
    queryFn: () => listJobPermissions(id ?? ''),
    enabled: Boolean(id)
  });
  const usersQuery = useQuery({
    queryKey: ['users', 'all'],
    queryFn: () => listUsers(0, 200),
    enabled: grantOpen
  });

  const revokeMutation = useMutation({
    mutationFn: (userId: string) => deleteJobPermission(id ?? '', userId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['jobPermissions', id] });
    }
  });

  const permissionMutation = useMutation({
    mutationFn: ({ userId, permissions }: { userId: string; permissions: PermissionType[] }) => {
      const request: JobPermissionRequest = { permissions };
      return replaceJobPermission(id ?? '', userId, request);
    },
    onSuccess: (_response, variables) => {
      void queryClient.invalidateQueries({ queryKey: ['jobPermissions', id] });
      setRowPermissions(current => {
        const next = { ...current };
        delete next[variables.userId];
        return next;
      });
      closeGrantDialog();
    },
    onError: error => {
      setPermissionError(errorMessage(error, 'Unable to update job permissions.'));
    }
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

  const grants = permissionsQuery.data ?? [];
  const existingUserIds = new Set(grants.map(grant => grant.userId).filter((userId): userId is string => Boolean(userId)));
  const availableUsers = (usersQuery.data?.content ?? []).filter(user => user.id && !existingUserIds.has(user.id));

  const rowPermissionsFor = (grant: JobPermissionResponse): PermissionType[] => {
    return rowPermissions[grant.userId ?? ''] ?? permissionsForGrant(grant);
  };

  const toggleRowPermission = (grant: JobPermissionResponse, permission: PermissionType) => {
    if (!grant.userId) {
      return;
    }
    const current = rowPermissionsFor(grant);
    const nextValues = current.includes(permission)
      ? current.filter(value => value !== permission)
      : [...current, permission];
    const nextSet = new Set(nextValues);
    const next = permissionTypes.filter(value => nextSet.has(value));
    setRowPermissions(currentRows => ({ ...currentRows, [grant.userId as string]: next }));
  };

  const submitGrant = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedUser?.id) {
      setPermissionError('Select a user before granting access.');
      return;
    }
    setPermissionError(undefined);
    permissionMutation.mutate({ userId: selectedUser.id, permissions: selectedPermissions });
  };

  const saveRowPermissions = (grant: JobPermissionResponse) => {
    if (!grant.userId) {
      return;
    }
    setPermissionError(undefined);
    permissionMutation.mutate({ userId: grant.userId, permissions: rowPermissionsFor(grant) });
  };

  if (jobQuery.isPending || permissionsQuery.isPending) {
    return <LoadingState label="Loading job permissions" />;
  }

  if (jobQuery.isError || !jobQuery.data) {
    return <Alert severity="error">{errorMessage(jobQuery.error, 'Unable to load this job.')}</Alert>;
  }

  if (permissionsQuery.isError) {
    return <Alert severity="error">{errorMessage(permissionsQuery.error, 'Unable to load job permissions.')}</Alert>;
  }

  const job = jobQuery.data;

  return (
    <Stack spacing={3}>
      <PageHeader
        title={`${job.name} permissions`}
        description="Manage which users can access this job."
        backLink={
          <Button component={RouterLink} to={`/jobs/${id}`} variant="text" startIcon={<ArrowBackIcon />}>
            Back to job
          </Button>
        }
      />
      <SurfaceSection
        title="Job permissions"
        actions={<Button variant="contained" onClick={openGrantDialog}>Grant access</Button>}
      >
        {grants.length === 0 ? (
          <EmptyState title="No users have explicit access to this job." />
        ) : (
          <TableContainer sx={{ overflowX: 'auto' }}>
            <Table aria-label="Job permissions" sx={{ minWidth: 760 }}>
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
                        onClick={() => saveRowPermissions(grant)}
                        disabled={permissionMutation.isPending || !grant.userId}
                        aria-label={`Save permissions for ${grant.username ?? 'unknown user'}`}
                      >
                        Save
                      </Button>
                      <Button
                        color="error"
                        size="small"
                        onClick={() => grant.userId && revokeMutation.mutate(grant.userId)}
                        disabled={revokeMutation.isPending}
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
        {permissionError && !grantOpen && (
          <Alert severity="error" sx={{ mt: 2 }}>{permissionError}</Alert>
        )}
        {revokeMutation.isError && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {errorMessage(revokeMutation.error, 'Unable to remove job permissions.')}
          </Alert>
        )}
      </SurfaceSection>
      <Dialog
        open={grantOpen}
        onClose={() => !permissionMutation.isPending && closeGrantDialog()}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Grant job access</DialogTitle>
        <DialogContent>
          <Stack component="form" id="grant-permission-form" noValidate onSubmit={submitGrant} spacing={2} sx={{ pt: 1 }}>
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
                      onChange={event => setSelectedPermissions(current => {
                        const nextValues = event.target.checked
                          ? [...current, permission]
                          : current.filter(value => value !== permission);
                        const nextSet = new Set(nextValues);
                        return permissionTypes.filter(value => nextSet.has(value));
                      })}
                    />
                  }
                />
              ))}
            </Stack>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeGrantDialog}>Cancel</Button>
          <Button type="submit" form="grant-permission-form" variant="contained" disabled={permissionMutation.isPending}>
            {permissionMutation.isPending ? 'Saving...' : 'Grant'}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
