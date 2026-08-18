import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as authApi from '../api/authApi';
import { AuthProvider } from './AuthContext';
import { useAuth } from './useAuth';

vi.mock('../api/authApi', () => ({
  getMe: vi.fn(),
  login: vi.fn(),
  logout: vi.fn()
}));

const mockedAuthApi = vi.mocked(authApi);

function SessionStatus() {
  const { status, user } = useAuth();
  return <output data-testid="session-status">{status}:{user?.username ?? ''}</output>;
}

function renderProvider() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <AuthProvider>
          <SessionStatus />
        </AuthProvider>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('AuthProvider', () => {
  it('loads an authenticated identity from /auth/me', async () => {
    mockedAuthApi.getMe.mockResolvedValue({
      id: 'user-id',
      username: 'operator',
      role: 'OPERATOR'
    });

    renderProvider();

    await waitFor(() => expect(screen.getByTestId('session-status')).toHaveTextContent('authenticated:operator'));
  });

  it.each([401, 403])('treats %s from /auth/me as anonymous', async status => {
    mockedAuthApi.getMe.mockRejectedValueOnce(new ApiError(status, 'Unauthorized', 'Authentication required'));

    renderProvider();

    await waitFor(() => expect(screen.getByTestId('session-status')).toHaveTextContent('anonymous:'));
  });
});
