import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { apiClient } from './client';
import { getDashboardSummary } from './dashboardApi';

describe('dashboardApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('requests the dashboard summary for an explicit window', async () => {
    mock.onGet('/dashboard/summary').reply(200, { totalJobs: 2, totalRuns: 0 });
    const window = { from: '2026-09-02T10:00:00.000Z', to: '2026-09-03T10:00:00.000Z' };

    await expect(getDashboardSummary(window)).resolves.toMatchObject({ totalJobs: 2 });
    expect(mock.history.get[0].params).toEqual(window);
  });
});
