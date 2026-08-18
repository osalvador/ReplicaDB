import { render, screen } from '@testing-library/react';
import { CssBaseline, ThemeProvider } from '@mui/material';
import { QueryClientProvider } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import App from './App';
import * as jobsApi from './api/jobsApi';
import { queryClient } from './api/queryClient';
import { AuthContext } from './auth/AuthContext';
import { theme } from './theme/theme';

vi.mock('./api/jobsApi', () => ({
  listJobs: vi.fn()
}));

const mockedJobsApi = vi.mocked(jobsApi);

function renderApp() {
  return render(
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          user: { id: 'user-id', username: 'operator', role: 'OPERATOR' },
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <App />
        </AuthContext.Provider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('App', () => {
  it('renders the router shell without throwing', async () => {
    mockedJobsApi.listJobs.mockResolvedValue({ content: [], page: 0, size: 50, totalElements: 0 });
    renderApp();

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
  });
});
