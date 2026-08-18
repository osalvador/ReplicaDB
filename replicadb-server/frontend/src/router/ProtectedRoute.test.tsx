import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { AuthContext } from '../auth/AuthContext';
import ProtectedRoute from './ProtectedRoute';

function renderGuard(status: 'loading' | 'anonymous' | 'authenticated') {
  const queryClient = new QueryClient();

  return render(
    <QueryClientProvider client={queryClient}>
      <AuthContext.Provider value={{
        status,
        user: status === 'authenticated' ? { id: 'user-id', username: 'operator', role: 'OPERATOR' } : undefined,
        login: vi.fn().mockResolvedValue(undefined),
        logout: vi.fn().mockResolvedValue(undefined)
      }}>
        <MemoryRouter initialEntries={['/private']}>
          <Routes>
            <Route element={<ProtectedRoute />}>
              <Route path="/private" element={<div>Private content</div>} />
            </Route>
            <Route path="/login" element={<div>Login destination</div>} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </QueryClientProvider>
  );
}

describe('ProtectedRoute', () => {
  it('shows a loading indicator while the session is bootstrapping', () => {
    renderGuard('loading');

    expect(screen.getByRole('progressbar', { name: 'Loading session' })).toBeInTheDocument();
  });

  it('redirects anonymous users to login', () => {
    renderGuard('anonymous');

    expect(screen.getByText('Login destination')).toBeInTheDocument();
  });

  it('renders the protected outlet for authenticated users', () => {
    renderGuard('authenticated');

    expect(screen.getByText('Private content')).toBeInTheDocument();
  });
});
