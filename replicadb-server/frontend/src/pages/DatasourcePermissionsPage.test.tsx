import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as datasourcesApi from '../api/datasourcesApi';
import type { DatasourcePermissionResponse, DatasourceResponse } from '../api/datasourcesApi';
import * as usersApi from '../api/usersApi';
import { theme } from '../theme/theme';
import DatasourcePermissionsPage from './DatasourcePermissionsPage';

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return {
    ...actual,
    getDatasource: vi.fn(),
    listDatasourcePermissions: vi.fn(),
    replaceDatasourcePermission: vi.fn(),
    revokeDatasourcePermission: vi.fn()
  };
});

vi.mock('../api/usersApi', () => ({
  listUsers: vi.fn()
}));

const mockedDatasourceApi = vi.mocked(datasourcesApi);
const mockedUsersApi = vi.mocked(usersApi);

const datasource: DatasourceResponse = {
  id: 'datasource-1',
  name: 'Warehouse',
  connectorType: 'postgres',
  safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/warehouse',
  technicalParams: { sslmode: 'require' },
  securityConfigured: true,
  capabilities: { sourceCapable: true, sinkCapable: true },
  canView: true,
  canUse: true,
  canEdit: true
};

const grants: DatasourcePermissionResponse[] = [
  { userId: 'user-1', username: 'operator', permissions: ['VIEW', 'USE'] }
];

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  mockedDatasourceApi.getDatasource.mockResolvedValue(datasource);
  mockedDatasourceApi.listDatasourcePermissions.mockResolvedValue(grants);
  mockedUsersApi.listUsers.mockResolvedValue({
    content: [
      { id: 'user-1', username: 'operator', role: 'OPERATOR', enabled: true },
      { id: 'user-2', username: 'viewer', role: 'VIEWER', enabled: true }
    ],
    page: 0,
    size: 200,
    totalElements: 2
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={['/datasources/datasource-1/permissions']}>
          <Routes>
            <Route path="/datasources/:id/permissions" element={<DatasourcePermissionsPage />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('DatasourcePermissionsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedDatasourceApi.replaceDatasourcePermission.mockResolvedValue({
      userId: 'user-1',
      username: 'operator',
      permissions: ['VIEW', 'USE']
    });
    mockedDatasourceApi.revokeDatasourcePermission.mockResolvedValue(undefined);
  });

  it('renders safe datasource metadata and permission categories', async () => {
    renderPage();

    expect(await screen.findByRole('heading', { name: 'Warehouse permissions' })).toBeInTheDocument();
    expect(screen.getByText('jdbc:postgresql://[REDACTED]/warehouse')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'VIEW permission for operator' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'USE permission for operator' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'EDIT permission for operator' })).not.toBeChecked();
    expect(screen.queryByText('password')).not.toBeInTheDocument();
  });

  it('replaces and revokes row permissions', async () => {
    renderPage();
    await screen.findByRole('checkbox', { name: 'VIEW permission for operator' });

    fireEvent.click(screen.getByRole('checkbox', { name: 'EDIT permission for operator' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save permissions for operator' }));
    await waitFor(() => expect(mockedDatasourceApi.replaceDatasourcePermission).toHaveBeenCalledWith(
      'datasource-1',
      'user-1',
      { permissions: ['VIEW', 'USE', 'EDIT'] }
    ));

    fireEvent.click(screen.getByRole('button', { name: 'Remove permissions for operator' }));
    await waitFor(() => expect(mockedDatasourceApi.revokeDatasourcePermission).toHaveBeenCalledWith(
      'datasource-1',
      'user-1'
    ));
  });

  it('requires a user and permission before granting new access', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: 'Grant access' }));

    fireEvent.click(screen.getByRole('button', { name: 'Grant' }));
    expect(await screen.findByText('Select a user before granting access.')).toBeInTheDocument();
    expect(mockedDatasourceApi.replaceDatasourcePermission).not.toHaveBeenCalled();
  });
});
