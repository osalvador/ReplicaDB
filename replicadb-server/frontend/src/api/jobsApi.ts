import type { components } from './schema';
import { apiClient } from './client';

type GeneratedJobDefinitionResponse = components['schemas']['JobDefinitionResponse'];
export type JobDefinitionResponse = Omit<
  GeneratedJobDefinitionResponse,
  'incrementalWatermarkColumn' | 'initialWatermarkValue' | 'modeWarning'
> & {
  incrementalWatermarkColumn?: string | null;
  initialWatermarkValue?: string | null;
  modeWarning?: string | null;
};
export type JobDefinitionPage = Omit<components['schemas']['PageResponseJobDefinitionResponse'], 'content'> & {
  content?: JobDefinitionResponse[];
};

export async function listJobs(page = 0, size = 50): Promise<JobDefinitionPage> {
  const response = await apiClient.get<JobDefinitionPage>('/jobs', {
    params: { page, size }
  });
  return response.data;
}

export async function getJob(id: string): Promise<JobDefinitionResponse> {
  const response = await apiClient.get<JobDefinitionResponse>(`/jobs/${id}`);
  return response.data;
}
