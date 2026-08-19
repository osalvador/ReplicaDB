import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, apiClient } from './client';
import { deleteSchedule, getSchedule, upsertSchedule } from './scheduleApi';

describe('scheduleApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('returns a configured schedule', async () => {
    const schedule = {
      jobDefinitionId: 'job-1',
      cronExpression: '0 0 * * * ?',
      timeZone: 'UTC',
      enabled: true,
      nextFireTime: '2026-08-18T17:00:00Z'
    };
    mock.onGet('/jobs/job-1/schedule').reply(200, schedule);

    await expect(getSchedule('job-1')).resolves.toEqual(schedule);
  });

  it('returns null when a schedule is not configured', async () => {
    mock.onGet('/jobs/job-1/schedule').reply(
      404,
      { title: 'Schedule not found', detail: 'No schedule is configured for this job.' },
      { 'content-type': 'application/problem+json' }
    );

    await expect(getSchedule('job-1')).resolves.toBeNull();
  });

  it('rethrows non-404 ApiErrors', async () => {
    mock.onGet('/jobs/job-1/schedule').reply(
      500,
      { title: 'Server error', detail: 'The schedule service is unavailable.' },
      { 'content-type': 'application/problem+json' }
    );

    await expect(getSchedule('job-1')).rejects.toMatchObject({
      status: 500,
      detail: 'The schedule service is unavailable.'
    });
  });

  it('upserts a schedule at the job schedule endpoint', async () => {
    const input = { cronExpression: '0 0 * * * ?', timeZone: 'UTC', enabled: true };
    mock.onPut('/jobs/job-1/schedule').reply(200, input);

    await expect(upsertSchedule('job-1', input)).resolves.toEqual(input);
    expect(JSON.parse(mock.history.put[0].data)).toEqual(input);
  });

  it('deletes a schedule at the job schedule endpoint', async () => {
    mock.onDelete('/jobs/job-1/schedule').reply(204);

    await expect(deleteSchedule('job-1')).resolves.toBeUndefined();
    expect(mock.history.delete[0].url).toBe('/jobs/job-1/schedule');
  });
});
