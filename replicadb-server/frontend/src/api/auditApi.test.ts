import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { apiClient } from './client';
import { listAuditEvents } from './auditApi';

describe('auditApi', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => mock.restore());

  it('lists audit events with supported filters', async () => {
    const page = { content: [], page: 0, size: 25, totalElements: 0 };
    mock.onGet('/audit').reply(200, page);

    await expect(listAuditEvents(0, 25, {
      actorUserId: 'user-1',
      action: 'JOB_UPDATED',
      resourceType: 'JOB_DEFINITION',
      resourceId: 'job-1',
      from: '2026-09-01T00:00:00Z',
      to: '2026-09-04T23:59:59Z'
    })).resolves.toEqual(page);
    expect(mock.history.get[0].params).toEqual({
      page: 0,
      size: 25,
      actorUserId: 'user-1',
      action: 'JOB_UPDATED',
      resourceType: 'JOB_DEFINITION',
      resourceId: 'job-1',
      from: '2026-09-01T00:00:00Z',
      to: '2026-09-04T23:59:59Z'
    });
  });
});
