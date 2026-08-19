import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as usersApi from '../api/usersApi';
import { theme } from '../theme/theme';
import UsersPage from './UsersPage';

vi.mock('../api/usersApi', () => ({
  createUser: vi.fn(),
  listUsers: vi.fn(),
  updateUserPassword: vi.fn(),
  updateUserRole: vi.fn()
}));

const mockedUsersApi = vi.mocked(usersApi);

const users = [
  {
    id: 'user-1',
    username: 'admin',
    role: 'ADMIN' as const,
    enabled: true,
    createdAt: '2026-08-19T10:00:00Z',
    updatedAt: '2026-08-19T10:00:00Z'
  },
  {
    id: 'user-2',
    username: 'viewer',
    role: 'VIEWER' as const,
    enabled: false,
    createdAt: '2026-08-19T10:00:00Z',
    updatedAt: '2026-08-19T10:00:00Z'
  }
];

function renderUsers(response = { content: users, page: 0, size: 50, totalElements: 2 }) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  mockedUsersApi.listUsers.mockResolvedValue(response);

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <UsersPage />
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

function selectRole(role: string) {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Role' }));
  fireEvent.click(screen.getByRole('option', { name: role }));
}

describe('UsersPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders users with role and enabled state', async () => {
    renderUsers();

    expect(await screen.findByText('admin')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 1, name: 'Users' })).toBeInTheDocument();
    expect(screen.getByText('ADMIN')).toBeInTheDocument();
    expect(screen.getByText('Yes')).toBeInTheDocument();
    expect(screen.getByText('No')).toBeInTheDocument();
  });

  it('renders an empty state when there are no users', async () => {
    renderUsers({ content: [], page: 0, size: 50, totalElements: 0 });

    expect(await screen.findByText('No users configured.')).toBeInTheDocument();
  });

  it('creates a user and closes the dialog', async () => {
    const createdUser = { ...users[1], username: 'operator', role: 'OPERATOR' as const, enabled: true };
    mockedUsersApi.createUser.mockResolvedValue(createdUser);

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Create user' }));
    await screen.findByRole('dialog');
    const passwordInput = screen.getByRole('dialog').querySelector('input[type="password"]');
    expect(passwordInput).not.toBeNull();
    fireEvent.change(passwordInput!, { target: { value: 'new-password' } });
    fireEvent.change(await screen.findByRole('textbox', { name: 'Username' }), { target: { value: 'operator' } });
    selectRole('OPERATOR');
    fireEvent.click(screen.getByRole('button', { name: 'Create' }));

    await waitFor(() => expect(mockedUsersApi.createUser).toHaveBeenCalledWith({
      username: 'operator',
      password: 'new-password',
      role: 'OPERATOR'
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('keeps the create dialog open when the username is rejected', async () => {
    mockedUsersApi.createUser.mockRejectedValue(
      new ApiError(409, 'Conflict', 'Username is already in use.')
    );

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Create user' }));
    await screen.findByRole('dialog');
    const passwordInput = screen.getByRole('dialog').querySelector('input[type="password"]');
    expect(passwordInput).not.toBeNull();
    fireEvent.change(passwordInput!, { target: { value: 'new-password' } });
    fireEvent.change(await screen.findByRole('textbox', { name: 'Username' }), { target: { value: 'admin' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create' }));

    expect(await screen.findByText('Username is already in use.')).toBeInTheDocument();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('edits a user role and enabled state', async () => {
    mockedUsersApi.updateUserRole.mockResolvedValue({ ...users[0], role: 'VIEWER', enabled: false });

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit admin' }));
    await screen.findByRole('dialog');
    expect(screen.getByRole('combobox', { name: 'Role' })).toHaveTextContent('ADMIN');
    expect(screen.getByRole('checkbox', { name: 'Enabled' })).toBeChecked();
    selectRole('VIEWER');
    fireEvent.click(screen.getByRole('checkbox', { name: 'Enabled' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(mockedUsersApi.updateUserRole).toHaveBeenCalledWith('user-1', {
      role: 'VIEWER',
      enabled: false
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('keeps the edit dialog open when the role update fails', async () => {
    mockedUsersApi.updateUserRole.mockRejectedValue(
      new ApiError(400, 'Invalid user', 'The role update could not be applied.')
    );

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit admin' }));
    await screen.findByRole('dialog');
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(await screen.findByText('The role update could not be applied.')).toBeInTheDocument();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('resets a user password and closes the dialog', async () => {
    mockedUsersApi.updateUserPassword.mockResolvedValue(users[0]);

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Reset password for admin' }));
    await screen.findByRole('dialog');
    const passwordInput = screen.getByRole('dialog').querySelector('input[type="password"]');
    expect(passwordInput).not.toBeNull();
    fireEvent.change(passwordInput!, { target: { value: 'new-password' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(mockedUsersApi.updateUserPassword).toHaveBeenCalledWith('user-1', {
      newPassword: 'new-password'
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('blocks a blank password before calling the API', async () => {
    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Reset password for admin' }));
    await screen.findByRole('dialog');
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(await screen.findByText('New password is required.')).toBeInTheDocument();
    expect(mockedUsersApi.updateUserPassword).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('keeps the password dialog open when the reset fails', async () => {
    mockedUsersApi.updateUserPassword.mockRejectedValue(
      new ApiError(400, 'Invalid password', 'The password does not meet policy.')
    );

    renderUsers();
    fireEvent.click(await screen.findByRole('button', { name: 'Reset password for admin' }));
    await screen.findByRole('dialog');
    const passwordInput = screen.getByRole('dialog').querySelector('input[type="password"]');
    expect(passwordInput).not.toBeNull();
    fireEvent.change(passwordInput!, { target: { value: 'new-password' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(await screen.findByText('The password does not meet policy.')).toBeInTheDocument();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
