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

const generatedEndpointTypes: RequiredApiPaths | undefined = undefined;
const generatedAdvancedJobFields: AdvancedJobFields | undefined = undefined;

describe('generated API schema', () => {
  it('contains the job and session endpoints', () => {
    expect(generatedEndpointTypes).toBeUndefined();
  });

  it('contains the advanced job fields', () => {
    expect(generatedAdvancedJobFields).toBeUndefined();
  });
});
