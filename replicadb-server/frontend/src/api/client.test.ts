import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
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
