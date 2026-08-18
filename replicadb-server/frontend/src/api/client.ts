import axios from 'axios';

export class ApiError extends Error {
  readonly status: number;
  readonly title: string;
  readonly detail: string;

  constructor(status: number, title: string, detail: string) {
    super(detail);
    this.name = 'ApiError';
    this.status = status;
    this.title = title;
    this.detail = detail;
  }
}

export const apiClient = axios.create({
  baseURL: '/api/v1',
  withCredentials: true,
  xsrfCookieName: 'XSRF-TOKEN',
  xsrfHeaderName: 'X-XSRF-TOKEN'
});

apiClient.interceptors.response.use(
  response => response,
  error => {
    if (axios.isAxiosError(error) && error.response) {
      const contentType = String(error.response.headers?.['content-type'] ?? '');
      const body = error.response.data as { title?: string; detail?: string } | undefined;
      if (contentType.includes('application/problem+json') && body) {
        throw new ApiError(
          error.response.status,
          body.title ?? 'Request failed',
          body.detail ?? 'The request could not be completed.'
        );
      }
    }

    throw error;
  }
);
