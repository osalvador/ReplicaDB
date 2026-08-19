import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { ThemeProvider } from '@mui/material';
import { describe, expect, it, vi } from 'vitest';
import { AuthContext } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import RequireRole from './RequireRole';

function renderGuard(role: 'ADMIN' | 'OPERATOR' | 'VIEWER') {
  const queryClient = new QueryClient();

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthContext.Provider value={{
          status: 'authenticated',
          user: { id: 'user-id', username: role.toLowerCase(), role },
          login: vi.fn().mockResolvedValue(undefined),
          logout: vi.fn().mockResolvedValue(undefined)
        }}>
          <MemoryRouter initialEntries={['/admin']}>
            <Routes>
              <Route element={<RequireRole role="ADMIN" />}>
                <Route path="/admin" element={<div>Admin content</div>} />
              </Route>
            </Routes>
          </MemoryRouter>
        </AuthContext.Provider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('RequireRole', () => {
  it('renders the protected outlet for an admin user', () => {
    renderGuard('ADMIN');

    expect(screen.getByText('Admin content')).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Not authorized' })).not.toBeInTheDocument();
  });

  it.each(['OPERATOR', 'VIEWER'] as const)('renders Not authorized for a %s user', role => {
    renderGuard(role);

    expect(screen.getByRole('heading', { name: 'Not authorized' })).toBeInTheDocument();
    expect(screen.queryByText('Admin content')).not.toBeInTheDocument();
  });
});
