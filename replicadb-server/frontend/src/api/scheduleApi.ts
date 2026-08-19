import type { components } from './schema';
import { ApiError, apiClient } from './client';

export async function getSchedule(
  jobId: string
): Promise<components['schemas']['JobScheduleResponse'] | null> {
  try {
    const response = await apiClient.get<components['schemas']['JobScheduleResponse']>(
      `/jobs/${jobId}/schedule`
    );
    return response.data;
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) {
      return null;
    }
    throw error;
  }
}

export async function upsertSchedule(
  jobId: string,
  input: components['schemas']['JobScheduleRequest']
): Promise<components['schemas']['JobScheduleResponse']> {
  const response = await apiClient.put<components['schemas']['JobScheduleResponse']>(
    `/jobs/${jobId}/schedule`,
    input
  );
  return response.data;
}

export async function deleteSchedule(jobId: string): Promise<void> {
  await apiClient.delete(`/jobs/${jobId}/schedule`);
}
