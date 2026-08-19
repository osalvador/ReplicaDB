import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, apiClient } from './client';
import {
  createUser,
  listUsers,
  updateUserPassword,
  updateUserRole
} from './usersApi';

const user = {
  id: 'user-1',
  username: 'operator',
  role: 'OPERATOR' as const,
  enabled: true,
  createdAt: '2026-08-19T10:00:00Z',
  updatedAt: '2026-08-19T10:00:00Z'
};

describe('usersApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('lists users with pagination parameters', async () => {
    const page = { content: [user], page: 1, size: 25, totalElements: 1 };
    mock.onGet('/users').reply(200, page);

    await expect(listUsers(1, 25)).resolves.toEqual(page);
    expect(mock.history.get[0].params).toEqual({ page: 1, size: 25 });
  });

  it('creates a user', async () => {
    const request = { username: 'operator', password: 'secret', role: 'OPERATOR' as const };
    mock.onPost('/users').reply(201, user);

    await expect(createUser(request)).resolves.toEqual(user);
    expect(JSON.parse(mock.history.post[0].data)).toEqual(request);
  });

  it('updates a user role and enabled state', async () => {
    const request = { role: 'VIEWER' as const, enabled: false };
    mock.onPut('/users/user-1').reply(200, { ...user, ...request });

    await expect(updateUserRole('user-1', request)).resolves.toEqual({ ...user, ...request });
    expect(JSON.parse(mock.history.put[0].data)).toEqual(request);
  });

  it('updates a user password', async () => {
    const request = { newPassword: 'new-secret' };
    mock.onPut('/users/user-1/password').reply(200, user);

    await expect(updateUserPassword('user-1', request)).resolves.toEqual(user);
    expect(JSON.parse(mock.history.put[0].data)).toEqual(request);
  });

  it('maps RFC 7807 failures to ApiError', async () => {
    mock.onPost('/users').reply(
      409,
      { title: 'Conflict', detail: 'Username is already in use.' },
      { 'content-type': 'application/problem+json' }
    );

    await expect(createUser({ username: 'operator', password: 'secret', role: 'OPERATOR' }))
      .rejects.toBeInstanceOf(ApiError);
    await expect(createUser({ username: 'operator', password: 'secret', role: 'OPERATOR' }))
      .rejects.toMatchObject({ status: 409, detail: 'Username is already in use.' });
  });
});
