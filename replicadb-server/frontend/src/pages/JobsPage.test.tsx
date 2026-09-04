import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as jobsApi from '../api/jobsApi';
import JobsPage from './JobsPage';
import { theme } from '../theme/theme';

vi.mock('../api/jobsApi', () => ({ listJobs: vi.fn() }));
const mockedJobsApi = vi.mocked(jobsApi);
const jobs = [{ id: 'job-1', name: 'Orders replication', sourceTable: 'orders', sinkTable: 'warehouse_orders', mode: 'incremental' }];

function renderJobs(response = { content: jobs, page: 0, size: 50, totalElements: 1 }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  mockedJobsApi.listJobs.mockResolvedValue(response);
  return render(<ThemeProvider theme={theme}><QueryClientProvider client={queryClient}><MemoryRouter initialEntries={['/jobs']}><Routes><Route path="/jobs" element={<JobsPage />} /><Route path="/jobs/:id" element={<div>Job detail destination</div>} /></Routes></MemoryRouter></QueryClientProvider></ThemeProvider>);
}

describe('JobsPage', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders the job catalog and links to job details', async () => {
    renderJobs();
    expect(await screen.findByRole('heading', { name: 'Job catalog' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Orders replication' })).toHaveAttribute('href', '/jobs/job-1');
    expect(screen.getByRole('link', { name: 'New job' })).toHaveAttribute('href', '/jobs/new');
  });

  it('opens the job detail when any row area is clicked', async () => {
    renderJobs();
    await screen.findByRole('row', { name: /Orders replication/ });

    fireEvent.click(screen.getByText('orders'));
    expect(await screen.findByText('Job detail destination')).toBeInTheDocument();
  });

  it.each(['Enter', ' '])('opens the job detail when a row is activated with %s', async key => {
    renderJobs();
    const jobRow = await screen.findByRole('row', { name: /Orders replication/ });

    fireEvent.keyDown(jobRow, { key });
    expect(await screen.findByText('Job detail destination')).toBeInTheDocument();
  });

  it('requests another catalog page', async () => {
    const fullPage = Array.from({ length: 50 }, (_, index) => ({ ...jobs[0], id: `job-${index}` }));
    renderJobs({ content: fullPage, page: 0, size: 50, totalElements: 100 });
    await screen.findByRole('heading', { name: 'Job catalog' });
    fireEvent.click(screen.getByRole('button', { name: 'Go to next page' }));
    await waitFor(() => expect(mockedJobsApi.listJobs).toHaveBeenCalledWith(1, 50));
  });

  it('shows an empty catalog state', async () => {
    renderJobs({ content: [], page: 0, size: 50, totalElements: 0 });
    expect(await screen.findByText('No jobs available.')).toBeInTheDocument();
  });
});
