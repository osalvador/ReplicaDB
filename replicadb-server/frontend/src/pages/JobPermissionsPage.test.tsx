import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as jobPermissionsApi from '../api/jobPermissionsApi';
import type { JobPermissionResponse } from '../api/jobPermissionsApi';
import * as jobsApi from '../api/jobsApi';
import * as usersApi from '../api/usersApi';
import { theme } from '../theme/theme';
import JobPermissionsPage from './JobPermissionsPage';

vi.mock('../api/jobPermissionsApi', () => ({
  deleteJobPermission: vi.fn(),
  listJobPermissions: vi.fn(),
  replaceJobPermission: vi.fn()
}));

vi.mock('../api/jobsApi', () => ({
  getJob: vi.fn()
}));

vi.mock('../api/usersApi', () => ({
  listUsers: vi.fn()
}));

const mockedJobPermissionsApi = vi.mocked(jobPermissionsApi);
const mockedJobsApi = vi.mocked(jobsApi);
const mockedUsersApi = vi.mocked(usersApi);

const job = {
  id: 'job-1',
  name: 'Orders replication'
};

const grants: JobPermissionResponse[] = [
  {
    userId: 'user-1',
    username: 'operator',
    permissions: ['VIEW', 'EXECUTE']
  },
  {
    userId: 'user-2',
    username: 'viewer',
    permissions: ['VIEW']
  }
];

function renderPage(permissionResponse = grants, permissionError?: unknown) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  mockedJobsApi.getJob.mockResolvedValue(job);
  if (permissionError) {
    mockedJobPermissionsApi.listJobPermissions.mockRejectedValue(permissionError);
  } else {
    mockedJobPermissionsApi.listJobPermissions.mockResolvedValue(permissionResponse);
  }

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={['/jobs/job-1/permissions']}>
          <Routes>
            <Route path="/jobs/:id/permissions" element={<JobPermissionsPage />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('JobPermissionsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders permission grants with the correct checked cells', async () => {
    renderPage();

    expect(await screen.findByRole('heading', { name: 'Orders replication permissions' })).toBeInTheDocument();
    expect(screen.getByText('operator')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'VIEW permission for operator' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'EDIT permission for operator' })).not.toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'EXECUTE permission for operator' })).toBeChecked();
    expect(screen.getByRole('link', { name: 'Back to job' })).toHaveAttribute('href', '/jobs/job-1');
  });

  it('renders an empty state when no users have access', async () => {
    renderPage([]);

    expect(await screen.findByText('No users have explicit access to this job.')).toBeInTheDocument();
  });

  it('revokes a user permission grant', async () => {
    mockedJobPermissionsApi.deleteJobPermission.mockResolvedValue();

    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: 'Remove permissions for operator' }));

    await waitFor(() => expect(mockedJobPermissionsApi.deleteJobPermission).toHaveBeenCalledWith('job-1', 'user-1'));
  });

  it('shows an inline error when the caller cannot read permissions', async () => {
    renderPage([], new ApiError(403, 'Forbidden', 'You need edit permission for this job.'));

    expect(await screen.findByRole('alert')).toHaveTextContent('You need edit permission for this job.');
    expect(screen.queryByRole('table', { name: 'Job permissions' })).not.toBeInTheDocument();
  });

  it('grants access to a new user and requests the documented user limit', async () => {
    const allUsers = [
      { ...grants[0], role: 'OPERATOR' as const, enabled: true },
      { ...grants[1], role: 'VIEWER' as const, enabled: true },
      { id: 'user-3', username: 'new-user', role: 'VIEWER' as const, enabled: true }
    ];
    mockedUsersApi.listUsers.mockResolvedValue({ content: allUsers, page: 0, size: 200, totalElements: 3 });
    mockedJobPermissionsApi.replaceJobPermission.mockResolvedValue({
      userId: 'user-3',
      username: 'new-user',
      permissions: ['VIEW']
    });

    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: 'Grant access' }));
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(mockedUsersApi.listUsers).toHaveBeenCalledWith(0, 200));
    fireEvent.mouseDown(within(dialog).getByRole('combobox', { name: 'User' }));
    expect(screen.queryByRole('option', { name: 'operator' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('option', { name: 'new-user' }));
    fireEvent.click(within(dialog).getByRole('checkbox', { name: 'VIEW' }));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Grant' }));

    await waitFor(() => expect(mockedJobPermissionsApi.replaceJobPermission).toHaveBeenCalledWith('job-1', 'user-3', {
      permissions: ['VIEW']
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('saves changed permissions directly from an existing row', async () => {
    mockedJobPermissionsApi.replaceJobPermission.mockResolvedValue({ ...grants[0], permissions: ['VIEW', 'EDIT', 'EXECUTE'] });

    renderPage();
    fireEvent.click(await screen.findByRole('checkbox', { name: 'EDIT permission for operator' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save permissions for operator' }));

    await waitFor(() => expect(mockedJobPermissionsApi.replaceJobPermission).toHaveBeenCalledWith('job-1', 'user-1', {
      permissions: ['VIEW', 'EDIT', 'EXECUTE']
    }));
  });

  it('allows granting a user with no permissions selected', async () => {
    mockedUsersApi.listUsers.mockResolvedValue({
      content: [{ id: 'user-3', username: 'new-user', role: 'VIEWER', enabled: true }],
      page: 0,
      size: 200,
      totalElements: 1
    });
    mockedJobPermissionsApi.replaceJobPermission.mockResolvedValue({ userId: 'user-3', username: 'new-user', permissions: [] });

    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: 'Grant access' }));
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(mockedUsersApi.listUsers).toHaveBeenCalledWith(0, 200));
    fireEvent.mouseDown(within(dialog).getByRole('combobox', { name: 'User' }));
    fireEvent.click(screen.getByRole('option', { name: 'new-user' }));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Grant' }));

    await waitFor(() => expect(mockedJobPermissionsApi.replaceJobPermission).toHaveBeenCalledWith('job-1', 'user-3', {
      permissions: []
    }));
  });

  it('keeps the grant dialog open when saving permissions fails', async () => {
    mockedUsersApi.listUsers.mockResolvedValue({
      content: [{ id: 'user-3', username: 'new-user', role: 'VIEWER', enabled: true }],
      page: 0,
      size: 200,
      totalElements: 1
    });
    mockedJobPermissionsApi.replaceJobPermission.mockRejectedValue(
      new ApiError(403, 'Forbidden', 'You cannot change permissions for this job.')
    );

    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: 'Grant access' }));
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(mockedUsersApi.listUsers).toHaveBeenCalledWith(0, 200));
    fireEvent.mouseDown(within(dialog).getByRole('combobox', { name: 'User' }));
    fireEvent.click(screen.getByRole('option', { name: 'new-user' }));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Grant' }));

    expect(await screen.findByText('You cannot change permissions for this job.')).toBeInTheDocument();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
