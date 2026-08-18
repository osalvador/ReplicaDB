import { describe, expect, it } from 'vitest';
import type { paths } from './schema';

type RequiredApiPaths = {
  jobs: paths['/api/v1/jobs']['get'];
  currentUser: paths['/api/v1/auth/me']['get'];
};

const generatedEndpointTypes: RequiredApiPaths | undefined = undefined;

describe('generated API schema', () => {
  it('contains the job and session endpoints', () => {
    expect(generatedEndpointTypes).toBeUndefined();
  });
});
