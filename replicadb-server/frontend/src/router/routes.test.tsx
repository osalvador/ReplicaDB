import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import * as jobsApi from '../api/jobsApi';
import * as jobPermissionsApi from '../api/jobPermissionsApi';
import * as datasourcesApi from '../api/datasourcesApi';
import * as usersApi from '../api/usersApi';
import * as auditApi from '../api/auditApi';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import { routeObjects } from './routes';

vi.mock('../api/jobsApi', async () => {
  const actual = await vi.importActual<typeof import('../api/jobsApi')>('../api/jobsApi');
  return {
    ...actual,
    listJobs: vi.fn(),
    getJob: vi.fn(),
    createJob: vi.fn(),
    updateJob: vi.fn()
  };
});

vi.mock('../api/jobPermissionsApi', () => ({
  listJobPermissions: vi.fn()
}));

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return {
    ...actual,
    createDatasource: vi.fn(),
    deleteDatasource: vi.fn(),
    getDatasource: vi.fn(),
    listDatasourcePermissions: vi.fn(),
    listDatasources: vi.fn(),
    replaceDatasourcePermission: vi.fn(),
    revokeDatasourcePermission: vi.fn(),
    updateDatasource: vi.fn()
  };
});

vi.mock('../api/usersApi', () => ({
  listUsers: vi.fn()
}));
vi.mock('../api/auditApi', async () => ({
  ...(await vi.importActual<typeof import('../api/auditApi')>('../api/auditApi')),
  listAuditEvents: vi.fn()
}));

const mockedJobsApi = vi.mocked(jobsApi);
const mockedJobPermissionsApi = vi.mocked(jobPermissionsApi);
const mockedDatasourcesApi = vi.mocked(datasourcesApi);
const mockedUsersApi = vi.mocked(usersApi);
const mockedAuditApi = vi.mocked(auditApi);

function renderAt(path: string, role: 'ADMIN' | 'OPERATOR' | 'VIEWER' = 'OPERATOR') {
  const memoryRouter = createMemoryRouter(routeObjects, {
    initialEntries: [path]
  });
  const queryClient = new QueryClient();

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          user: { id: 'user-id', username: role.toLowerCase(), role },
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <RouterProvider router={memoryRouter} />
        </AuthContext.Provider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('route shell', () => {
  it('renders the login page at /login', () => {
    renderAt('/login');

    expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument();
  });

  it('renders the dashboard at the protected root', async () => {
    mockedJobsApi.listJobs.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });
    renderAt('/');

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
  });

  it('renders the job form at the create route', () => {
    renderAt('/jobs/new');

    expect(screen.getByRole('heading', { name: 'New job' })).toBeInTheDocument();
  });

  it('renders the datasource catalog at the protected route', async () => {
    mockedDatasourcesApi.listDatasources.mockResolvedValue({ content: [], page: 0, size: 25, totalElements: 0 });
    renderAt('/datasources');

    expect(await screen.findByRole('heading', { name: 'Datasources' })).toBeInTheDocument();
  });

  it('renders datasource creation and permissions routes for admins', async () => {
    renderAt('/datasources/new', 'ADMIN');
    expect(screen.getByRole('heading', { name: 'New datasource' })).toBeInTheDocument();

    mockedDatasourcesApi.getDatasource.mockResolvedValue({ id: 'datasource-1', name: 'Warehouse' });
    mockedDatasourcesApi.listDatasourcePermissions.mockResolvedValue([]);
    renderAt('/datasources/datasource-1/permissions', 'ADMIN');
    expect(await screen.findByRole('heading', { name: 'Warehouse permissions' })).toBeInTheDocument();
  });

  it('renders the job form at the edit route', async () => {
    mockedJobsApi.getJob.mockResolvedValue({ id: 'job-1', name: 'Existing job' });
    renderAt('/jobs/job-1/edit');

    expect(await screen.findByRole('heading', { name: 'Edit job' })).toBeInTheDocument();
  });

  it('renders the users administration route for admins', async () => {
    mockedUsersApi.listUsers.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });
    renderAt('/users', 'ADMIN');

    expect(await screen.findByRole('heading', { name: 'Users' })).toBeInTheDocument();
  });

  it('renders the audit route for admins', async () => {
    mockedAuditApi.listAuditEvents.mockResolvedValue({ content: [], page: 0, size: 25, totalElements: 0 });
    mockedUsersApi.listUsers.mockResolvedValue({ content: [], page: 0, size: 100, totalElements: 0 });
    renderAt('/audit', 'ADMIN');

    expect(await screen.findByRole('heading', { name: 'Audit' })).toBeInTheDocument();
  });

  it('renders the profile route for every authenticated role', () => {
    renderAt('/profile', 'OPERATOR');

    expect(screen.getByRole('heading', { name: 'My profile' })).toBeInTheDocument();
    expect(screen.getByLabelText('Username')).toHaveValue('operator');
    expect(screen.getByLabelText('Role')).toHaveValue('OPERATOR');
    expect(screen.getByText('Contact an administrator to change your password for now.')).toBeInTheDocument();
  });

  it('renders the job permissions route for admins', async () => {
    mockedJobsApi.getJob.mockResolvedValue({ id: 'job-1', name: 'Orders replication' });
    mockedJobPermissionsApi.listJobPermissions.mockResolvedValue([]);
    renderAt('/jobs/job-1/permissions', 'ADMIN');

    expect(await screen.findByRole('heading', { name: 'Orders replication permissions' })).toBeInTheDocument();
  });

  it.each(['/audit', '/users', '/jobs/job-1/permissions', '/datasources/new', '/datasources/datasource-1/permissions'])('blocks %s for non-admin users', path => {
    renderAt(path, 'OPERATOR');

    expect(screen.getByRole('heading', { name: 'Not authorized' })).toBeInTheDocument();
  });
});
