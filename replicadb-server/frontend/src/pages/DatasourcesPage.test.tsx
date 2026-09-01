import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as datasourcesApi from '../api/datasourcesApi';
import type { DatasourceResponse } from '../api/datasourcesApi';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import { ThemeProvider } from '@mui/material';
import DatasourcesPage from './DatasourcesPage';

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return {
    ...actual,
    deleteDatasource: vi.fn(),
    listDatasources: vi.fn()
  };
});

const mockedApi = vi.mocked(datasourcesApi);

const datasourceRows: DatasourceResponse[] = [
  {
    id: 'datasource-1',
    name: 'Warehouse',
    connectorType: 'postgres',
    safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/warehouse',
    technicalParams: { sslmode: 'require' },
    securityConfigured: true,
    capabilities: {
      sourceCapable: true,
      sinkCapable: true,
      sourceModes: ['complete', 'incremental'],
      sinkModes: ['complete', 'incremental']
    },
    canView: true,
    canUse: true,
    canEdit: true
  },
  {
    id: 'datasource-2',
    name: 'Archive files',
    connectorType: 'file',
    safeConnectDisplay: 'file:///var/replica/archive',
    technicalParams: {},
    securityConfigured: false,
    capabilities: { sourceCapable: true, sinkCapable: true, singleJobOnly: true },
    canView: true,
    canUse: false,
    canEdit: false
  }
];

function renderPage(role: 'ADMIN' | 'OPERATOR' | 'VIEWER' = 'ADMIN') {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  mockedApi.listDatasources.mockResolvedValue({
    content: datasourceRows,
    page: 0,
    size: 25,
    totalElements: datasourceRows.length
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          user: { id: 'user-1', username: role.toLowerCase(), role },
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <MemoryRouter>
            <DatasourcesPage />
          </MemoryRouter>
        </AuthContext.Provider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('DatasourcesPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedApi.deleteDatasource.mockResolvedValue(undefined);
  });

  it('renders safe metadata, capabilities, and configured state without secrets', async () => {
    renderPage();

    expect(await screen.findByRole('heading', { name: 'Datasources' })).toBeInTheDocument();
    expect(screen.getByText('Warehouse')).toBeInTheDocument();
    expect(screen.getByText('jdbc:postgresql://[REDACTED]/warehouse')).toBeInTheDocument();
    expect(screen.getByText('Configured')).toBeInTheDocument();
    expect(screen.getAllByText('Source').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Sink').length).toBeGreaterThan(0);
    expect(screen.queryByText('transient-value')).not.toBeInTheDocument();
  });

  it('passes source and sink role filters to the API', async () => {
    renderPage();

    await screen.findByText('Warehouse');
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Role filter' }));
    fireEvent.click(screen.getByRole('option', { name: 'Source capable' }));

    await waitFor(() => expect(mockedApi.listDatasources).toHaveBeenLastCalledWith(0, 25, 'source'));
  });

  it('deletes a datasource through the confirmation dialog and invalidates the catalog', async () => {
    renderPage();
    await screen.findByText('Warehouse');

    fireEvent.click(screen.getByRole('button', { name: 'Delete Warehouse' }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Delete datasource' }));

    await waitFor(() => expect(mockedApi.deleteDatasource).toHaveBeenCalledWith('datasource-1'));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('hides admin actions for non-admin users while retaining editable profiles', async () => {
    renderPage('OPERATOR');

    await screen.findByText('Warehouse');
    expect(screen.queryByRole('link', { name: 'New datasource' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Permissions for Warehouse' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete Warehouse' })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Edit Warehouse' })).toBeInTheDocument();
  });
});
