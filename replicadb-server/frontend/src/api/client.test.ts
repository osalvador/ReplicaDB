import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { getCsrf, login } from './authApi';
import { ApiError, apiClient } from './client';

describe('apiClient', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('sends requests with session credentials enabled', async () => {
    mock.onGet('/session').reply(200, { authenticated: true });

    await apiClient.get('/session');

    expect(mock.history.get[0].withCredentials).toBe(true);
  });

  it('keeps the API same-origin and configured for the browser CSRF cookie', () => {
    expect(apiClient.defaults.baseURL).toBe('/api/v1');
    expect(apiClient.defaults.withCredentials).toBe(true);
    expect(apiClient.defaults.xsrfCookieName).toBe('XSRF-TOKEN');
    expect(apiClient.defaults.xsrfHeaderName).toBe('X-XSRF-TOKEN');
  });

  it('bootstraps CSRF before sending login credentials', async () => {
    mock.onGet('/auth/csrf').reply(200, {
      headerName: 'X-XSRF-TOKEN',
      parameterName: '_csrf',
      token: 'test-token'
    });
    mock.onPost('/auth/login').reply(200, { id: 'user-id', username: 'operator', role: 'OPERATOR' });

    await getCsrf();
    await login('operator', 'password');

    expect(mock.history.get.map(request => request.url)).toEqual(['/auth/csrf', '/auth/csrf']);
    expect(mock.history.post[0].url).toBe('/auth/login');
    expect(mock.history.post[0].withCredentials).toBe(true);
    expect(JSON.parse(mock.history.post[0].data)).toEqual({ username: 'operator', password: 'password' });
  });

  it('maps RFC 7807 responses to ApiError', async () => {
    mock.onGet('/missing').reply(
      404,
      { title: 'Job not found', detail: 'The requested job does not exist.' },
      { 'content-type': 'application/problem+json' }
    );

    await expect(apiClient.get('/missing')).rejects.toBeInstanceOf(ApiError);

    try {
      await apiClient.get('/missing');
    } catch (error) {
      expect(error).toMatchObject({
        status: 404,
        title: 'Job not found',
        detail: 'The requested job does not exist.'
      });
    }
  });

  it('passes successful responses through unchanged', async () => {
    const payload = { content: [], page: 0, size: 50, totalElements: 0 };
    mock.onGet('/jobs').reply(200, payload);

    const response = await apiClient.get('/jobs');

    expect(response.data).toEqual(payload);
  });
});
