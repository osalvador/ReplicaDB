import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { apiClient } from './client';
import { cancelRun, retryRun, triggerRun } from './runsApi';

describe('runsApi mutations', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('sends a UUID v4 idempotency key when triggering a run', async () => {
    mock.onPost('/jobs/job-1/runs').reply(201, { id: 'run-1' });

    await triggerRun('job-1');

    expect(mock.history.post[0].headers?.['Idempotency-Key']).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
    );
  });

  it('generates a fresh idempotency key for sequential triggers', async () => {
    mock.onPost('/jobs/job-1/runs').reply(201, { id: 'run-1' });

    await triggerRun('job-1');
    await triggerRun('job-1');

    const firstKey = mock.history.post[0].headers?.['Idempotency-Key'];
    const secondKey = mock.history.post[1].headers?.['Idempotency-Key'];
    expect(firstKey).toBeTruthy();
    expect(secondKey).toBeTruthy();
    expect(firstKey).not.toBe(secondKey);
  });

  it('returns the cancellation warning', async () => {
    const response = {
      runId: 'run-1',
      status: 'CANCEL_REQUESTED',
      warning: 'Cancellation was requested; the worker may still be stopping.'
    };
    mock.onPost('/runs/run-1/cancel').reply(200, response);

    await expect(cancelRun('run-1')).resolves.toMatchObject({ warning: response.warning });
  });

  it('posts a retry request and returns the new run', async () => {
    const response = { id: 'run-2', jobDefinitionId: 'job-1', status: 'PENDING' };
    mock.onPost('/runs/run-1/retry').reply(201, response);

    await expect(retryRun('run-1')).resolves.toEqual(response);
    expect(mock.history.post[0].url).toBe('/runs/run-1/retry');
  });
});
