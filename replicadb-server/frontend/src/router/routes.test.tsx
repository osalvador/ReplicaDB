import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import * as jobsApi from '../api/jobsApi';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import { routeObjects } from './routes';

vi.mock('../api/jobsApi', () => ({
  listJobs: vi.fn(),
  getJob: vi.fn(),
  createJob: vi.fn(),
  updateJob: vi.fn()
}));

const mockedJobsApi = vi.mocked(jobsApi);

function renderAt(path: string) {
  const memoryRouter = createMemoryRouter(routeObjects, {
    initialEntries: [path]
  });
  const queryClient = new QueryClient();

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <RouterProvider router={memoryRouter} />
        </AuthContext.Provider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('route shell', () => {
  it('renders the login page at /login', () => {
    renderAt('/login');

    expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument();
  });

  it('renders the dashboard at the protected root', async () => {
    mockedJobsApi.listJobs.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });
    renderAt('/');

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
  });

  it('renders the job form at the create route', () => {
    renderAt('/jobs/new');

    expect(screen.getByRole('heading', { name: 'New job' })).toBeInTheDocument();
  });

  it('renders the job form at the edit route', async () => {
    mockedJobsApi.getJob.mockResolvedValue({ id: 'job-1', name: 'Existing job' });
    renderAt('/jobs/job-1/edit');

    expect(await screen.findByRole('heading', { name: 'Edit job' })).toBeInTheDocument();
  });
});
