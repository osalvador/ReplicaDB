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

export type JobDefinitionFormInput = {
  name: string;
  sourceConnect: string;
  sourceUser?: string;
  sourcePassword?: string;
  sourceTable: string;
  sourceWhere?: string;
  sourceAuthMode?: string;
  sourceAuthPrincipalId?: string;
  sourceAuthLoginHint?: string;
  sourceAuthClientCertificate?: string;
  sourceAuthClientKey?: string;
  sourceConnectionParams?: Record<string, string>;
  sourceColumns?: string;
  sourceQuery?: string;
  sinkConnect: string;
  sinkUser?: string;
  sinkPassword?: string;
  sinkTable: string;
  sinkAuthMode?: string;
  sinkAuthPrincipalId?: string;
  sinkAuthLoginHint?: string;
  sinkAuthClientCertificate?: string;
  sinkAuthClientKey?: string;
  sinkConnectionParams?: Record<string, string>;
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
const normalizeOptionalMap = (value?: Record<string, string>): Record<string, string> | undefined => {
  if (!value || Object.keys(value).length === 0) {
    return undefined;
  }
  return value;
};

export function toJobDefinitionRequest(
  input: JobDefinitionMutationInput
): components['schemas']['JobDefinitionRequest'] {
  const mode = input.mode;
  const sourceQuery = normalizeOptionalString(input.sourceQuery);
  const sourceSelection = sourceQuery
    ? { sourceQuery }
    : { sourceTable: normalizeOptionalString(input.sourceTable) };
  const request = {
    name: input.name,
    sourceConnect: input.sourceConnect,
    sourceUser: normalizeOptionalString(input.sourceUser),
    sourcePassword: normalizeOptionalString(input.sourcePassword),
    sourceWhere: normalizeOptionalString(input.sourceWhere),
    sourceAuthMode: normalizeOptionalString(input.sourceAuthMode),
    sourceAuthPrincipalId: normalizeOptionalString(input.sourceAuthPrincipalId),
    sourceAuthLoginHint: normalizeOptionalString(input.sourceAuthLoginHint),
    sourceAuthClientCertificate: normalizeOptionalString(input.sourceAuthClientCertificate),
    sourceAuthClientKey: normalizeOptionalString(input.sourceAuthClientKey),
    sourceConnectionParams: normalizeOptionalMap(input.sourceConnectionParams),
    sourceColumns: normalizeOptionalString(input.sourceColumns),
    ...sourceSelection,
    sinkConnect: input.sinkConnect,
    sinkUser: normalizeOptionalString(input.sinkUser),
    sinkPassword: normalizeOptionalString(input.sinkPassword),
    sinkTable: input.sinkTable,
    sinkAuthMode: normalizeOptionalString(input.sinkAuthMode),
    sinkAuthPrincipalId: normalizeOptionalString(input.sinkAuthPrincipalId),
    sinkAuthLoginHint: normalizeOptionalString(input.sinkAuthLoginHint),
    sinkAuthClientCertificate: normalizeOptionalString(input.sinkAuthClientCertificate),
    sinkAuthClientKey: normalizeOptionalString(input.sinkAuthClientKey),
    sinkConnectionParams: normalizeOptionalMap(input.sinkConnectionParams),
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
