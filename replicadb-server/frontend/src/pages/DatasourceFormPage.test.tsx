import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as datasourcesApi from '../api/datasourcesApi';
import type { DatasourceResponse } from '../api/datasourcesApi';
import { theme } from '../theme/theme';
import DatasourceFormPage from './DatasourceFormPage';

vi.mock('../api/datasourcesApi', async () => {
  const actual = await vi.importActual<typeof import('../api/datasourcesApi')>('../api/datasourcesApi');
  return {
    ...actual,
    createDatasource: vi.fn(),
    getDatasource: vi.fn(),
    updateDatasource: vi.fn()
  };
});

const mockedApi = vi.mocked(datasourcesApi);

const datasource: DatasourceResponse = {
  id: 'datasource-1',
  name: 'Warehouse',
  connectorType: 'postgres',
  safeConnectDisplay: 'jdbc:postgresql://warehouse.example:5432/warehouse',
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
};

function renderPage(path: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path="/datasources/new" element={<DatasourceFormPage />} />
            <Route path="/datasources/:id/edit" element={<DatasourceFormPage />} />
            <Route path="/datasources/:id" element={<div>Saved datasource</div>} />
            <Route path="/datasources" element={<div>Datasource catalog</div>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

function fillPostgresConnection() {
  fireEvent.change(screen.getByLabelText(/^Datasource name/), { target: { value: 'Warehouse' } });
  fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'warehouse.example' } });
  fireEvent.change(screen.getByLabelText('Port'), { target: { value: '5432' } });
  fireEvent.change(screen.getByLabelText('Database / SID or Service Name'), { target: { value: 'warehouse' } });
}

describe('DatasourceFormPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedApi.createDatasource.mockResolvedValue({ ...datasource, id: 'datasource-new' });
    mockedApi.updateDatasource.mockResolvedValue(datasource);
  });

  it('creates a datasource with transient security and non-secret technical parameters', async () => {
    renderPage('/datasources/new');
    fillPostgresConnection();
    fireEvent.change(screen.getByLabelText('Datasource user'), { target: { value: 'operator' } });
    fireEvent.change(screen.getByLabelText('Datasource password'), { target: { value: 'transient-value' } });
    fireEvent.change(screen.getByLabelText('Extra JDBC parameters'), {
      target: { value: 'sslmode=require\napplicationName=ReplicaDB' }
    });
    fireEvent.click(screen.getByRole('button', { name: 'Create datasource' }));

    await waitFor(() => expect(mockedApi.createDatasource).toHaveBeenCalledTimes(1));
    expect(mockedApi.createDatasource).toHaveBeenCalledWith({
      name: 'Warehouse',
      connectorType: 'postgres',
      technicalParams: { applicationName: 'ReplicaDB', sslmode: 'require' },
      security: {
        connect: 'jdbc:postgresql://warehouse.example:5432/warehouse',
        user: 'operator',
        password: 'transient-value'
      },
      clearSecurityKeys: []
    });
    expect(await screen.findByText('Saved datasource')).toBeInTheDocument();
  });

  it('does not hydrate write-only security values during edit and preserves blank updates', async () => {
    mockedApi.getDatasource.mockResolvedValue(datasource);
    renderPage('/datasources/datasource-1/edit');

    expect(await screen.findByDisplayValue('Warehouse')).toBeInTheDocument();
    expect(screen.getByLabelText('Datasource password')).toHaveValue('');
    expect(screen.getByLabelText('Datasource user')).toHaveValue('');
    expect(screen.queryByText('transient-value')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Save datasource' }));

    await waitFor(() => expect(mockedApi.updateDatasource).toHaveBeenCalledWith(
      'datasource-1',
      expect.objectContaining({
        name: 'Warehouse',
        connectorType: 'postgres',
        technicalParams: { sslmode: 'require' },
        security: {},
        clearSecurityKeys: []
      })
    ));
  });

  it('sends explicit clear keys without reading the existing password', async () => {
    mockedApi.getDatasource.mockResolvedValue(datasource);
    renderPage('/datasources/datasource-1/edit');
    await screen.findByDisplayValue('Warehouse');

    fireEvent.click(screen.getByLabelText('User password'));
    fireEvent.click(screen.getByRole('button', { name: 'Save datasource' }));
    await waitFor(() => expect(mockedApi.updateDatasource).toHaveBeenCalledWith(
      'datasource-1',
      expect.objectContaining({ clearSecurityKeys: ['password'], security: {} })
    ));
  });

  it('renders S3 and Mongo-specific controls', () => {
    renderPage('/datasources/new');
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Datasource data source type' }));
    fireEvent.click(screen.getByRole('option', { name: 'Amazon S3' }));
    expect(screen.getByLabelText('S3 endpoint')).toBeInTheDocument();
    expect(screen.getByLabelText('Bucket')).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'S3 access key' })).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Datasource data source type' }));
    fireEvent.click(screen.getByRole('option', { name: 'MongoDB Atlas (SRV)' }));
    expect(screen.getByLabelText(/^Datasource MongoDB URI/)).toBeInTheDocument();
  });
});
