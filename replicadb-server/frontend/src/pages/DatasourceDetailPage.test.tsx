import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as datasourcesApi from '../api/datasourcesApi';
import type { DatasourceResponse } from '../api/datasourcesApi';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import DatasourceDetailPage from './DatasourceDetailPage';

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return { ...actual, deleteDatasource: vi.fn(), getDatasource: vi.fn() };
});

const mockedApi = vi.mocked(datasourcesApi);
const datasource: DatasourceResponse = {
  id: 'datasource-1',
  name: 'Warehouse',
  connectorType: 'postgres',
  safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/warehouse',
  technicalParams: { applicationName: 'ReplicaDB', sslmode: 'require' },
  securityConfigured: true,
  capabilities: {
    sourceCapable: true,
    sinkCapable: true,
    sourceModes: ['complete', 'incremental'],
    sinkModes: ['complete'],
    sourceQuery: true
  },
  canView: true,
  canUse: true,
  canEdit: true
};

describe('DatasourceDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedApi.getDatasource.mockResolvedValue(datasource);
    mockedApi.deleteDatasource.mockResolvedValue(undefined);
  });

  it('shows safe connection metadata, capabilities, and technical parameters only', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <ThemeProvider theme={theme}>
        <QueryClientProvider client={queryClient}>
          <AuthContext.Provider value={{
            status: 'authenticated',
            user: { id: 'user-1', username: 'admin', role: 'ADMIN' },
            login: vi.fn().mockResolvedValue(undefined),
            logout: vi.fn().mockResolvedValue(undefined)
          }}>
            <MemoryRouter initialEntries={['/datasources/datasource-1']}>
              <Routes>
                <Route path="/datasources/:id" element={<DatasourceDetailPage />} />
                <Route path="/datasources" element={<div>Datasource catalog</div>} />
              </Routes>
            </MemoryRouter>
          </AuthContext.Provider>
        </QueryClientProvider>
      </ThemeProvider>
    );

    expect(await screen.findByRole('heading', { name: 'Warehouse' })).toBeInTheDocument();
    expect(screen.getByText('jdbc:postgresql://[REDACTED]/warehouse')).toBeInTheDocument();
    expect(screen.getByText('Source capable')).toBeInTheDocument();
    expect(screen.getByText('Sink capable')).toBeInTheDocument();
    expect(screen.getByText('complete, incremental')).toBeInTheDocument();
    expect(screen.getByText(/applicationName=ReplicaDB/)).toHaveTextContent('sslmode=require');
    expect(screen.queryByText('transient-value')).not.toBeInTheDocument();
  });

  it('deletes the datasource from its detail actions and returns to the catalog', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
    });
    render(
      <ThemeProvider theme={theme}>
        <QueryClientProvider client={queryClient}>
          <AuthContext.Provider value={{
            status: 'authenticated',
            user: { id: 'user-1', username: 'admin', role: 'ADMIN' },
            login: vi.fn().mockResolvedValue(undefined),
            logout: vi.fn().mockResolvedValue(undefined)
          }}>
            <MemoryRouter initialEntries={['/datasources/datasource-1']}>
              <Routes>
                <Route path="/datasources/:id" element={<DatasourceDetailPage />} />
                <Route path="/datasources" element={<div>Datasource catalog</div>} />
              </Routes>
            </MemoryRouter>
          </AuthContext.Provider>
        </QueryClientProvider>
      </ThemeProvider>
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Delete datasource' }));
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('A profile referenced by a job cannot be deleted.');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete datasource' }));

    await waitFor(() => expect(mockedApi.deleteDatasource).toHaveBeenCalledWith('datasource-1'));
    expect(await screen.findByText('Datasource catalog')).toBeInTheDocument();
  });
});
