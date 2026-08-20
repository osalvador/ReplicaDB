import { describe, expect, it } from 'vitest';
import type { components, paths } from './schema';

type RequiredApiPaths = {
  jobs: paths['/api/v1/jobs']['get'];
  currentUser: paths['/api/v1/auth/me']['get'];
};

type AdvancedJobFields = Pick<components['schemas']['JobDefinitionRequest'],
  | 'sourceAuthMode'
  | 'sourceConnectionParams'
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
const generatedAdvancedJobFields: AdvancedJobFields | undefined = undefined;
const generatedRetryPolicyFields: RetryPolicyFields | undefined = undefined;
const generatedRetryPolicyResponseFields: RetryPolicyResponseFields | undefined = undefined;
const generatedPublicRunFields: PublicRunFields | undefined = undefined;
const leaseTokenIsNotPublic: LeaseTokenIsNotPublic = true;

describe('generated API schema', () => {
  it('contains the job and session endpoints', () => {
    expect(generatedEndpointTypes).toBeUndefined();
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
