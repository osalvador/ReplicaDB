import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { apiClient, ApiError } from './client';
import {
  createDatasource,
  datasourceQueryKeys,
  deleteDatasource,
  getDatasource,
  invalidateDatasourceQueries,
  listDatasourcePermissions,
  listDatasources,
  replaceDatasourcePermission,
  revokeDatasourcePermission,
  toDatasourceRequest,
  updateDatasource
} from './datasourcesApi';

describe('datasourcesApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('normalizes blank security and technical values while preserving explicit clears', () => {
    const request = toDatasourceRequest({
      name: 'warehouse',
      connectorType: 'postgres',
      technicalParams: { sslmode: 'require', empty: ' ' },
      security: { connect: 'jdbc:postgresql://host/db', password: ' ', token: 'transient-value' },
      clearSecurityKeys: ['password', '', 'password']
    });

    expect(request).toEqual({
      name: 'warehouse',
      connectorType: 'postgres',
      technicalParams: { sslmode: 'require' },
      security: { connect: 'jdbc:postgresql://host/db', token: 'transient-value' },
      clearSecurityKeys: ['password']
    });
  });

  it('lists datasources with pagination and a source or sink capability filter', async () => {
    mock.onGet('/datasources').reply(200, {
      content: [{ id: 'source-1', connectorType: 'postgres', canUse: true }],
      page: 2,
      size: 10,
      totalElements: 11
    });

    await expect(listDatasources(2, 10, 'source')).resolves.toMatchObject({
      page: 2,
      size: 10,
      totalElements: 11
    });
    expect(mock.history.get[0].params).toEqual({ page: 2, size: 10, role: 'source' });
  });

  it('performs datasource CRUD without modeling secrets in responses', async () => {
    const response = {
      id: 'datasource-1',
      name: 'warehouse',
      connectorType: 'postgres',
      safeConnectDisplay: 'jdbc:postgresql://[REDACTED]/db',
      technicalParams: { sslmode: 'require' },
      securityConfigured: true,
      canView: true,
      canUse: true,
      canEdit: true
    };
    mock.onPost('/datasources').reply(201, response);
    mock.onGet('/datasources/datasource-1').reply(200, response);
    mock.onPut('/datasources/datasource-1').reply(200, response);
    mock.onDelete('/datasources/datasource-1').reply(204);

    await expect(createDatasource({
      name: 'warehouse',
      connectorType: 'postgres',
      security: { connect: 'jdbc:postgresql://host/db', password: 'transient-value' }
    })).resolves.toEqual(response);
    await expect(getDatasource('datasource-1')).resolves.toEqual(response);
    await expect(updateDatasource('datasource-1', {
      name: 'warehouse',
      connectorType: 'postgres',
      security: { password: '' },
      clearSecurityKeys: ['password']
    })).resolves.toEqual(response);
    await expect(deleteDatasource('datasource-1')).resolves.toBeUndefined();

    expect(JSON.parse(mock.history.post[0].data).security.password).toBe('transient-value');
    expect(JSON.parse(mock.history.put[0].data)).toMatchObject({ clearSecurityKeys: ['password'] });
    expect(response).not.toHaveProperty('security');
    expect(response).not.toHaveProperty('encryptedSecurity');
    expect(response).not.toHaveProperty('keyVersion');
  });

  it('uses the datasource ACL endpoints and returns only permission categories', async () => {
    mock.onGet('/datasources/datasource-1/permissions').reply(200, [
      { userId: 'user-1', username: 'operator', permissions: ['VIEW', 'USE'] }
    ]);
    mock.onPut('/datasources/datasource-1/permissions/user-1').reply(200, {
      userId: 'user-1',
      username: 'operator',
      permissions: ['EDIT']
    });
    mock.onDelete('/datasources/datasource-1/permissions/user-1').reply(204);

    await expect(listDatasourcePermissions('datasource-1')).resolves.toHaveLength(1);
    await expect(replaceDatasourcePermission('datasource-1', 'user-1', {
      permissions: ['EDIT']
    })).resolves.toMatchObject({ permissions: ['EDIT'] });
    await expect(revokeDatasourcePermission('datasource-1', 'user-1')).resolves.toBeUndefined();

    expect(mock.history.put[0].url).toBe('/datasources/datasource-1/permissions/user-1');
    expect(mock.history.delete[0].url).toBe('/datasources/datasource-1/permissions/user-1');
  });

  it('preserves RFC 7807 errors and exposes stable query keys for invalidation', async () => {
    mock.onPost('/datasources').reply(409, {
      title: 'Conflict',
      detail: 'Datasource name is already in use'
    }, { 'content-type': 'application/problem+json' });

    await expect(createDatasource({ name: 'duplicate', connectorType: 'postgres' }))
      .rejects.toBeInstanceOf(ApiError);
    await expect(createDatasource({ name: 'duplicate', connectorType: 'postgres' }))
      .rejects.toMatchObject({ status: 409, detail: 'Datasource name is already in use' });

    expect(datasourceQueryKeys.list(0, 50, 'sink')).toEqual([
      'datasources', 'list', { page: 0, size: 50, role: 'sink' }
    ]);
    expect(datasourceQueryKeys.detail('datasource-1')).toEqual([
      'datasources', 'detail', 'datasource-1'
    ]);
    expect(datasourceQueryKeys.permissions('datasource-1')).toEqual([
      'datasources', 'permissions', 'datasource-1'
    ]);

    const queryClient = { invalidateQueries: vi.fn().mockResolvedValue(undefined) };
    await invalidateDatasourceQueries(queryClient);
    expect(queryClient.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['datasources'] });
  });
});
