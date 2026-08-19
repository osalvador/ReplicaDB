import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, apiClient } from './client';
import {
  deleteJobPermission,
  listJobPermissions,
  replaceJobPermission
} from './jobPermissionsApi';

const permission = {
  userId: 'user-1',
  username: 'operator',
  permissions: ['VIEW', 'EXECUTE'] as const
};

describe('jobPermissionsApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('lists permissions for a job', async () => {
    mock.onGet('/jobs/job-1/permissions').reply(200, [permission]);

    await expect(listJobPermissions('job-1')).resolves.toEqual([permission]);
  });

  it('returns an empty permission list when no users are granted', async () => {
    mock.onGet('/jobs/job-1/permissions').reply(200, []);

    await expect(listJobPermissions('job-1')).resolves.toEqual([]);
  });

  it('replaces a user permission set', async () => {
    const request = { permissions: ['VIEW', 'EDIT'] as ('VIEW' | 'EDIT')[] };
    mock.onPut('/jobs/job-1/permissions/user-1').reply(200, { ...permission, permissions: request.permissions });

    await expect(replaceJobPermission('job-1', 'user-1', request))
      .resolves.toEqual({ ...permission, permissions: request.permissions });
    expect(JSON.parse(mock.history.put[0].data)).toEqual(request);
  });

  it('deletes a user permission set', async () => {
    mock.onDelete('/jobs/job-1/permissions/user-1').reply(204);

    await expect(deleteJobPermission('job-1', 'user-1')).resolves.toBeUndefined();
    expect(mock.history.delete[0].url).toBe('/jobs/job-1/permissions/user-1');
  });

  it('maps forbidden responses to ApiError', async () => {
    mock.onGet('/jobs/job-1/permissions').reply(
      403,
      { title: 'Forbidden', detail: 'You need edit permission for this job.' },
      { 'content-type': 'application/problem+json' }
    );

    await expect(listJobPermissions('job-1')).rejects.toBeInstanceOf(ApiError);
    await expect(listJobPermissions('job-1')).rejects.toMatchObject({
      status: 403,
      detail: 'You need edit permission for this job.'
    });
  });
});
