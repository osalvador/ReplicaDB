import type { QueryClient } from '@tanstack/react-query';
import type { components } from './schema';
import { apiClient } from './client';

export type DatasourceResponse = components['schemas']['DatasourceResponse'];
export type DatasourceRequest = components['schemas']['DatasourceRequest'];
export type DatasourcePermissionRequest = components['schemas']['DatasourcePermissionRequest'];
export type DatasourcePermissionResponse = components['schemas']['DatasourcePermissionResponse'];
export type DatasourceRole = 'source' | 'sink';
export type DatasourcePage = Omit<components['schemas']['PageResponseDatasourceResponse'], 'content'> & {
  content?: DatasourceResponse[];
};

export type DatasourceMutationInput = {
  name: string;
  connectorType: string;
  technicalParams?: Record<string, string>;
  security?: Record<string, string>;
  clearSecurityKeys?: string[];
};

export const datasourceQueryKeys = {
  all: ['datasources'] as const,
  list: (page: number, size: number, role?: DatasourceRole) =>
    ['datasources', 'list', { page, size, role }] as const,
  detail: (id: string) => ['datasources', 'detail', id] as const,
  permissions: (id: string) => ['datasources', 'permissions', id] as const
};

export function toDatasourceRequest(input: DatasourceMutationInput): DatasourceRequest {
  return {
    name: input.name,
    connectorType: input.connectorType,
    technicalParams: normalizeMap(input.technicalParams),
    security: normalizeMap(input.security),
    clearSecurityKeys: normalizeKeys(input.clearSecurityKeys)
  };
}

export async function listDatasources(
  page = 0,
  size = 50,
  role?: DatasourceRole
): Promise<DatasourcePage> {
  const response = await apiClient.get<DatasourcePage>('/datasources', {
    params: { page, size, role }
  });
  return response.data;
}

export async function getDatasource(id: string): Promise<DatasourceResponse> {
  const response = await apiClient.get<DatasourceResponse>(`/datasources/${id}`);
  return response.data;
}

export async function createDatasource(input: DatasourceMutationInput): Promise<DatasourceResponse> {
  const response = await apiClient.post<DatasourceResponse>('/datasources', toDatasourceRequest(input));
  return response.data;
}

export async function updateDatasource(
  id: string,
  input: DatasourceMutationInput
): Promise<DatasourceResponse> {
  const response = await apiClient.put<DatasourceResponse>(
    `/datasources/${id}`,
    toDatasourceRequest(input)
  );
  return response.data;
}

export async function deleteDatasource(id: string): Promise<void> {
  await apiClient.delete(`/datasources/${id}`);
}

export async function listDatasourcePermissions(id: string): Promise<DatasourcePermissionResponse[]> {
  const response = await apiClient.get<DatasourcePermissionResponse[]>(
    `/datasources/${id}/permissions`
  );
  return response.data;
}

export async function replaceDatasourcePermission(
  datasourceId: string,
  userId: string,
  request: DatasourcePermissionRequest
): Promise<DatasourcePermissionResponse> {
  const response = await apiClient.put<DatasourcePermissionResponse>(
    `/datasources/${datasourceId}/permissions/${userId}`,
    request
  );
  return response.data;
}

export async function revokeDatasourcePermission(datasourceId: string, userId: string): Promise<void> {
  await apiClient.delete(`/datasources/${datasourceId}/permissions/${userId}`);
}

export function invalidateDatasourceQueries(queryClient: Pick<QueryClient, 'invalidateQueries'>) {
  return queryClient.invalidateQueries({ queryKey: datasourceQueryKeys.all });
}

function normalizeMap(value?: Record<string, string>): Record<string, string> | undefined {
  if (!value) {
    return undefined;
  }
  const entries = Object.entries(value).filter(([, entryValue]) => entryValue.trim() !== '');
  return entries.length === 0 ? undefined : Object.fromEntries(entries);
}

function normalizeKeys(value?: string[]): string[] | undefined {
  if (!value) {
    return undefined;
  }
  const keys = [...new Set(value.filter(key => key.trim() !== ''))];
  return keys.length === 0 ? undefined : keys;
}
