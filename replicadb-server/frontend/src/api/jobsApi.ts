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
  sourceDatasourceId?: string | null;
  sinkDatasourceId?: string | null;
  sourceDatasource?: GeneratedJobDefinitionResponse['sourceDatasource'] | null;
  sinkDatasource?: GeneratedJobDefinitionResponse['sinkDatasource'] | null;
};
export type JobDefinitionPage = Omit<components['schemas']['PageResponseJobDefinitionResponse'], 'content'> & {
  content?: JobDefinitionResponse[];
};

export type JobDefinitionFormInput = {
  name: string;
  sourceDatasourceId: string;
  sourceDatasourceUseEnabled: boolean;
  sourceTable: string;
  sourceWhere?: string;
  sourceColumns?: string;
  sourceQuery?: string;
  sinkDatasourceId: string;
  sinkDatasourceUseEnabled: boolean;
  sinkTable: string;
  sinkColumns?: string;
  sinkStagingSchema?: string;
  sinkStagingTable?: string;
  sinkDisableEscape: boolean;
  sinkDisableTruncate: boolean;
  mode: 'complete' | 'complete-atomic' | 'incremental';
  jobs: number;
  incrementalWatermarkColumn?: string;
  initialWatermarkValue?: string;
  fetchSize: number;
  bandwidthThrottling: number;
  verbose: boolean;
  maxAttempts: number;
  retryBackoffSeconds: number;
  automaticRetryEnabled: boolean;
};

export type JobDefinitionMutationInput =
  | JobDefinitionFormInput
  | components['schemas']['JobDefinitionRequest'];

const normalizeOptionalString = (value?: string): string | undefined => value === '' ? undefined : value;

export function toJobDefinitionRequest(
  input: JobDefinitionMutationInput
): components['schemas']['JobDefinitionRequest'] {
  const mode = input.mode;
  const sourceQuery = normalizeOptionalString(input.sourceQuery);
  const sourceTable = normalizeOptionalString(input.sourceTable);
  const request = {
    name: input.name,
    sourceDatasourceId: input.sourceDatasourceId,
    sourceDatasourceUseEnabled: input.sourceDatasourceUseEnabled,
    sourceWhere: normalizeOptionalString(input.sourceWhere),
    sourceColumns: normalizeOptionalString(input.sourceColumns),
    ...(sourceQuery ? { sourceQuery } : { sourceTable }),
    sinkDatasourceId: input.sinkDatasourceId,
    sinkDatasourceUseEnabled: input.sinkDatasourceUseEnabled,
    sinkTable: input.sinkTable,
    sinkColumns: normalizeOptionalString(input.sinkColumns),
    sinkStagingSchema: normalizeOptionalString(input.sinkStagingSchema),
    sinkStagingTable: normalizeOptionalString(input.sinkStagingTable),
    sinkDisableEscape: input.sinkDisableEscape,
    sinkDisableTruncate: input.sinkDisableTruncate,
    mode,
    jobs: input.jobs,
    fetchSize: input.fetchSize,
    bandwidthThrottling: input.bandwidthThrottling,
    verbose: input.verbose,
    maxAttempts: input.maxAttempts,
    retryBackoffSeconds: input.retryBackoffSeconds,
    automaticRetryEnabled: input.automaticRetryEnabled
  } satisfies components['schemas']['JobDefinitionRequest'];

  if (mode === 'incremental') {
    return {
      ...request,
      incrementalWatermarkColumn: input.incrementalWatermarkColumn,
      initialWatermarkValue: input.initialWatermarkValue
    };
  }

  return request;
}

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

export async function createJob(input: JobDefinitionMutationInput): Promise<JobDefinitionResponse> {
  const response = await apiClient.post<JobDefinitionResponse>('/jobs', toJobDefinitionRequest(input));
  return response.data;
}

export async function updateJob(id: string, input: JobDefinitionMutationInput): Promise<JobDefinitionResponse> {
  const response = await apiClient.put<JobDefinitionResponse>(`/jobs/${id}`, toJobDefinitionRequest(input));
  return response.data;
}
