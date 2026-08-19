import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { ApiError } from '../api/client';
import {
  createUser,
  listUsers,
  updateUserPassword,
  updateUserRole,
  type PasswordUpdate,
  type RoleUpdate,
  type UserRequest,
  type UserResponse
} from '../api/usersApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

const userRoles = ['ADMIN', 'OPERATOR', 'VIEWER'] as const;
type UserRole = typeof userRoles[number];

type CreateUserForm = Pick<UserRequest, 'username' | 'password' | 'role'>;

const emptyCreateUser: CreateUserForm = {
  username: '',
  password: '',
  role: 'VIEWER'
};

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to update users.';
}

export default function UsersPage() {
  const queryClient = useQueryClient();
  const [page, setPage] = useState(0);
  const [createOpen, setCreateOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateUserForm>(emptyCreateUser);
  const [createError, setCreateError] = useState<string>();
  const [editingUser, setEditingUser] = useState<UserResponse>();
  const [editForm, setEditForm] = useState<RoleUpdate>({ role: 'VIEWER', enabled: true });
  const [editError, setEditError] = useState<string>();
  const [passwordUser, setPasswordUser] = useState<UserResponse>();
  const [passwordValue, setPasswordValue] = useState('');
  const [passwordError, setPasswordError] = useState<string>();

  const size = 50;
  const usersQuery = useQuery({
    queryKey: ['users', page, size],
    queryFn: () => listUsers(page, size)
  });

  const createMutation = useMutation({
    mutationFn: (request: UserRequest) => createUser(request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['users'] });
      setCreateOpen(false);
      setCreateForm(emptyCreateUser);
      setCreateError(undefined);
    },
    onError: error => {
      setCreateError(errorMessage(error));
    }
  });

  const editMutation = useMutation({
    mutationFn: ({ id, request }: { id: string; request: RoleUpdate }) => updateUserRole(id, request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['users'] });
      setEditingUser(undefined);
      setEditError(undefined);
    },
    onError: error => {
      setEditError(errorMessage(error));
    }
  });

  const passwordMutation = useMutation({
    mutationFn: ({ id, request }: { id: string; request: PasswordUpdate }) => updateUserPassword(id, request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['users'] });
      setPasswordUser(undefined);
      setPasswordValue('');
      setPasswordError(undefined);
    },
    onError: error => {
      setPasswordValue('');
      setPasswordError(errorMessage(error));
    }
  });

  const openCreateDialog = () => {
    setCreateForm(emptyCreateUser);
    setCreateError(undefined);
    setCreateOpen(true);
  };

  const submitCreate = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!createForm.username.trim() || !createForm.password) {
      setCreateError('Username and password are required.');
      return;
    }
    setCreateError(undefined);
    createMutation.mutate({
      username: createForm.username.trim(),
      password: createForm.password,
      role: createForm.role
    });
  };

  const openEditDialog = (user: UserResponse) => {
    setEditingUser(user);
    setEditForm({ role: user.role ?? 'VIEWER', enabled: user.enabled ?? false });
    setEditError(undefined);
  };

  const submitEdit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!editingUser?.id) {
      return;
    }
    setEditError(undefined);
    editMutation.mutate({ id: editingUser.id, request: editForm });
  };

  const openPasswordDialog = (user: UserResponse) => {
    setPasswordUser(user);
    setPasswordValue('');
    setPasswordError(undefined);
  };

  const submitPassword = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!passwordUser?.id || !passwordValue) {
      setPasswordError('New password is required.');
      return;
    }
    setPasswordError(undefined);
    passwordMutation.mutate({ id: passwordUser.id, request: { newPassword: passwordValue } });
  };

  if (usersQuery.isPending) {
    return <LoadingState label="Loading users" />;
  }

  if (usersQuery.isError) {
    return <Alert severity="error">Unable to load users.</Alert>;
  }

  const users = usersQuery.data.content ?? [];
  const totalElements = usersQuery.data.totalElements ?? 0;
  const paginationCount = users.length < size ? page * size + users.length : totalElements;

  return (
    <>
      <PageHeader
        title="Users"
        description="Manage local accounts and roles."
        actions={<Button variant="contained" onClick={openCreateDialog}>Create user</Button>}
      />
      <SurfaceSection title="Local users">
        <TableContainer sx={{ overflowX: 'auto' }}>
          <Table aria-label="Users" sx={{ minWidth: 760 }}>
            <TableHead>
              <TableRow>
                <TableCell>Username</TableCell>
                <TableCell>Role</TableCell>
                <TableCell>Enabled</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {users.map((user: UserResponse) => (
                <TableRow key={user.id ?? user.username}>
                  <TableCell>{user.username ?? 'Unknown'}</TableCell>
                  <TableCell>{user.role ?? 'Unknown'}</TableCell>
                  <TableCell>{user.enabled ? 'Yes' : 'No'}</TableCell>
                  <TableCell align="right">
                    <Button size="small" onClick={() => openEditDialog(user)} aria-label={`Edit ${user.username}`}>
                      Edit
                    </Button>
                    <Button size="small" onClick={() => openPasswordDialog(user)} aria-label={`Reset password for ${user.username}`}>
                      Reset password
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {users.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4}>
                    <EmptyState title="No users configured." />
                  </TableCell>
                </TableRow>
              )}
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
      </SurfaceSection>

      <Dialog
        open={createOpen}
        onClose={() => !createMutation.isPending && setCreateOpen(false)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Create user</DialogTitle>
        <DialogContent>
          <Stack component="form" id="create-user-form" onSubmit={submitCreate} spacing={2} sx={{ pt: 1 }}>
            {createError && <Alert severity="error">{createError}</Alert>}
            <TextField
              autoFocus
              label="Username"
              value={createForm.username}
              onChange={event => setCreateForm(current => ({ ...current, username: event.target.value }))}
              required
              fullWidth
            />
            <TextField
              label="Password"
              type="password"
              value={createForm.password}
              onChange={event => setCreateForm(current => ({ ...current, password: event.target.value }))}
              required
              fullWidth
            />
            <TextField
              select
              label="Role"
              value={createForm.role}
              onChange={event => setCreateForm(current => ({ ...current, role: event.target.value as UserRole }))}
              fullWidth
            >
              {userRoles.map(role => <MenuItem key={role} value={role}>{role}</MenuItem>)}
            </TextField>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button type="submit" form="create-user-form" variant="contained" disabled={createMutation.isPending}>
            {createMutation.isPending ? 'Creating...' : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={Boolean(passwordUser)}
        onClose={() => !passwordMutation.isPending && (setPasswordUser(undefined), setPasswordValue(''), setPasswordError(undefined))}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Reset password</DialogTitle>
        <DialogContent>
          <Stack component="form" id="reset-password-form" noValidate onSubmit={submitPassword} spacing={2} sx={{ pt: 1 }}>
            {passwordError && <Alert severity="error">{passwordError}</Alert>}
            <TextField
              label="New password"
              type="password"
              value={passwordValue}
              onChange={event => setPasswordValue(event.target.value)}
              required
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setPasswordUser(undefined);
            setPasswordValue('');
            setPasswordError(undefined);
          }}>Cancel</Button>
          <Button type="submit" form="reset-password-form" variant="contained" disabled={passwordMutation.isPending}>
            {passwordMutation.isPending ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={Boolean(editingUser)}
        onClose={() => !editMutation.isPending && setEditingUser(undefined)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Edit user</DialogTitle>
        <DialogContent>
          <Stack component="form" id="edit-user-form" onSubmit={submitEdit} spacing={2} sx={{ pt: 1 }}>
            {editError && <Alert severity="error">{editError}</Alert>}
            <TextField
              select
              label="Role"
              value={editForm.role}
              onChange={event => setEditForm(current => ({ ...current, role: event.target.value as UserRole }))}
              fullWidth
            >
              {userRoles.map(role => <MenuItem key={role} value={role}>{role}</MenuItem>)}
            </TextField>
            <FormControlLabel
              control={
                <Switch
                  checked={editForm.enabled}
                  onChange={event => setEditForm(current => ({ ...current, enabled: event.target.checked }))}
                />
              }
              label="Enabled"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditingUser(undefined)}>Cancel</Button>
          <Button type="submit" form="edit-user-form" variant="contained" disabled={editMutation.isPending}>
            {editMutation.isPending ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
