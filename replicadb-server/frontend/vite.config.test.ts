// @vitest-environment node

import { describe, expect, it } from 'vitest';
import config from './vite.config';

describe('Vite development proxy', () => {
  it('routes API and OpenAPI requests to the local server', () => {
    const proxy = config.server?.proxy;

    expect(proxy?.['/api/v1']).toMatchObject({
      target: 'http://localhost:8080',
      changeOrigin: true
    });
    expect(proxy?.['/v3/api-docs']).toMatchObject({
      target: 'http://localhost:8080',
      changeOrigin: true
    });
  });
});
