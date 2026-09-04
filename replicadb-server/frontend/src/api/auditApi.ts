import type { components } from './schema';
import { apiClient } from './client';

export type AuditEventResponse = components['schemas']['AuditEventResponse'];
export type AuditAction = NonNullable<AuditEventResponse['action']>;
export type AuditResourceType = NonNullable<AuditEventResponse['resourceType']>;
export type AuditPage = Omit<components['schemas']['PageResponseAuditEventResponse'], 'content'> & {
  content?: AuditEventResponse[];
};

export type AuditFilters = {
  actorUserId?: string;
  action?: AuditAction;
  resourceType?: AuditResourceType;
  resourceId?: string;
  from?: string;
  to?: string;
};

export const auditQueryKeys = {
  all: ['audit'] as const,
  list: (page: number, size: number, filters: AuditFilters) => ['audit', 'list', { page, size, ...filters }] as const
};

export async function listAuditEvents(
  page = 0,
  size = 25,
  filters: AuditFilters = {}
): Promise<AuditPage> {
  const response = await apiClient.get<AuditPage>('/audit', {
    params: { page, size, ...filters }
  });
  return response.data;
}
