import type { components } from './schema';
import { apiClient } from './client';

export type GeneratedJobRunResponse = components['schemas']['JobRunResponse'];
export type JobRunResponse = Omit<GeneratedJobRunResponse,
  'previousRunId' | 'executorIdentity' | 'leaseUntil' | 'heartbeatAt' | 'startedAt' |
  'finishedAt' | 'rowsProcessed' | 'durationMillis' | 'committedWatermark' | 'errorMessage' |
  'cancellationWarning'> & {
  previousRunId?: string | null;
  executorIdentity?: string | null;
  leaseUntil?: string | null;
  heartbeatAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  rowsProcessed?: number | null;
  durationMillis?: number | null;
  committedWatermark?: string | null;
  errorMessage?: string | null;
  cancellationWarning?: string | null;
};
export type JobRunStatus = NonNullable<GeneratedJobRunResponse['status']>;
export type JobRunPage = Omit<components['schemas']['PageResponseJobRunResponse'], 'content'> & {
  content?: JobRunResponse[];
};
export type RunLogResponse = components['schemas']['RunLogResponse'];

export async function listJobRuns(jobId: string, page = 0, size = 50): Promise<JobRunPage> {
  const response = await apiClient.get<JobRunPage>(`/jobs/${jobId}/runs`, {
    params: { page, size }
  });
  return response.data;
}

export async function getRun(id: string): Promise<JobRunResponse> {
  const response = await apiClient.get<JobRunResponse>(`/runs/${id}`);
  return response.data;
}

export async function getRunLog(id: string): Promise<RunLogResponse> {
  const response = await apiClient.get<RunLogResponse>(`/runs/${id}/log`);
  return response.data;
}

export async function triggerRun(jobId: string): Promise<JobRunResponse> {
  const response = await apiClient.post<JobRunResponse>(
    `/jobs/${jobId}/runs`,
    undefined,
    { headers: { 'Idempotency-Key': crypto.randomUUID() } }
  );
  return response.data;
}

export async function cancelRun(id: string): Promise<components['schemas']['CancellationResponse']> {
  const response = await apiClient.post<components['schemas']['CancellationResponse']>(`/runs/${id}/cancel`);
  return response.data;
}

export async function retryRun(id: string): Promise<JobRunResponse> {
  const response = await apiClient.post<JobRunResponse>(`/runs/${id}/retry`);
  return response.data;
}
