import { describe, expect, it } from 'vitest';
import type { components, paths } from './schema';

type RequiredApiPaths = {
  jobs: paths['/api/v1/jobs']['get'];
  datasources: paths['/api/v1/datasources']['get'];
  datasourcePermissions: paths['/api/v1/datasources/{datasourceId}/permissions']['get'];
  currentUser: paths['/api/v1/auth/me']['get'];
};

type DatasourceRequestFields = Pick<components['schemas']['DatasourceRequest'],
  'name' | 'connectorType' | 'technicalParams' | 'security' | 'clearSecurityKeys'>;
type DatasourceResponseFields = Pick<components['schemas']['DatasourceResponse'],
  'safeConnectDisplay' | 'technicalParams' | 'securityConfigured' | 'capabilities' | 'canUse' | 'canEdit'>;
type DatasourceResponseHasNoSecrets =
  'security' extends keyof components['schemas']['DatasourceResponse'] ? false :
  'encryptedSecurity' extends keyof components['schemas']['DatasourceResponse'] ? false :
  'keyVersion' extends keyof components['schemas']['DatasourceResponse'] ? false : true;
type JobRequestHasNoInlineCredentials =
  'sourceConnect' extends keyof components['schemas']['JobDefinitionRequest'] ? false :
  'sinkConnect' extends keyof components['schemas']['JobDefinitionRequest'] ? false :
  'sourcePassword' extends keyof components['schemas']['JobDefinitionRequest'] ? false :
  'sinkPassword' extends keyof components['schemas']['JobDefinitionRequest'] ? false : true;

type AdvancedJobFields = Pick<components['schemas']['JobDefinitionRequest'],
  | 'sourceDatasourceId'
  | 'sourceDatasourceUseEnabled'
  | 'sinkDatasourceId'
  | 'sinkDatasourceUseEnabled'
  | 'sourceQuery'
  | 'sinkStagingTable'
  | 'fetchSize'
  | 'bandwidthThrottling'
  | 'verbose'>;

type RetryPolicyFields = Pick<components['schemas']['JobDefinitionRequest'],
  'maxAttempts' | 'retryBackoffSeconds' | 'automaticRetryEnabled'>;
type RetryPolicyResponseFields = Pick<components['schemas']['JobDefinitionResponse'],
  'maxAttempts' | 'retryBackoffSeconds' | 'automaticRetryEnabled'>;
type PublicRunFields = Pick<components['schemas']['JobRunResponse'], 'availableAt'>;
type LeaseTokenIsNotPublic = 'leaseToken' extends keyof components['schemas']['JobRunResponse'] ? false : true;

const generatedEndpointTypes: RequiredApiPaths | undefined = undefined;
const generatedDatasourceRequest: DatasourceRequestFields | undefined = undefined;
const generatedDatasourceResponse: DatasourceResponseFields | undefined = undefined;
const generatedAdvancedJobFields: AdvancedJobFields | undefined = undefined;
const generatedRetryPolicyFields: RetryPolicyFields | undefined = undefined;
const generatedRetryPolicyResponseFields: RetryPolicyResponseFields | undefined = undefined;
const generatedPublicRunFields: PublicRunFields | undefined = undefined;
const leaseTokenIsNotPublic: LeaseTokenIsNotPublic = true;
const datasourceResponseHasNoSecrets: DatasourceResponseHasNoSecrets = true;
const jobRequestHasNoInlineCredentials: JobRequestHasNoInlineCredentials = true;

describe('generated API schema', () => {
  it('contains the job and session endpoints', () => {
    expect(generatedEndpointTypes).toBeUndefined();
  });

  it('contains datasource request/response fields without secret response fields', () => {
    expect(generatedDatasourceRequest).toBeUndefined();
    expect(generatedDatasourceResponse).toBeUndefined();
    expect(datasourceResponseHasNoSecrets).toBe(true);
    expect(jobRequestHasNoInlineCredentials).toBe(true);
  });

  it('contains the advanced job fields', () => {
    expect(generatedAdvancedJobFields).toBeUndefined();
  });

  it('contains retry policy and public run timing fields without lease identity', () => {
    expect(generatedRetryPolicyFields).toBeUndefined();
    expect(generatedRetryPolicyResponseFields).toBeUndefined();
    expect(generatedPublicRunFields).toBeUndefined();
    expect(leaseTokenIsNotPublic).toBe(true);
  });
});
