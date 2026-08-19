import type { components } from './schema';
import { apiClient } from './client';

export type JobPermissionRequest = components['schemas']['JobPermissionRequest'];
export type JobPermissionResponse = components['schemas']['JobPermissionResponse'];

export async function listJobPermissions(jobId: string): Promise<JobPermissionResponse[]> {
  const response = await apiClient.get<JobPermissionResponse[]>(`/jobs/${jobId}/permissions`);
  return response.data;
}

export async function replaceJobPermission(
  jobId: string,
  userId: string,
  request: JobPermissionRequest
): Promise<JobPermissionResponse> {
  const response = await apiClient.put<JobPermissionResponse>(
    `/jobs/${jobId}/permissions/${userId}`,
    request
  );
  return response.data;
}

export async function deleteJobPermission(jobId: string, userId: string): Promise<void> {
  await apiClient.delete(`/jobs/${jobId}/permissions/${userId}`);
}
