import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as authApi from '../api/authApi';
import { AuthProvider } from '../auth/AuthContext';
import AppLayout from './AppLayout';

vi.mock('../api/authApi', () => ({
  getMe: vi.fn(),
  login: vi.fn(),
  logout: vi.fn()
}));

const mockedAuthApi = vi.mocked(authApi);

function renderLayout(queryClient: QueryClient) {
  const router = createMemoryRouter([
    {
      path: '/',
      element: <AppLayout />,
      children: [{ index: true, element: <div>Dashboard content</div> }]
    },
    { path: '/login', element: <div>Login destination</div> }
  ], { initialEntries: ['/'] });

  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <RouterProvider router={router} />
      </AuthProvider>
    </QueryClientProvider>
  );
}

describe('AppLayout', () => {
  beforeEach(() => {
    mockedAuthApi.getMe.mockResolvedValue({ id: 'user-id', username: 'operator', role: 'OPERATOR' });
    mockedAuthApi.logout.mockResolvedValue(undefined);
  });

  it('logs out, clears cached data, and navigates to login', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } }
    });
    queryClient.setQueryData(['jobs'], { content: ['private data'] });
    renderLayout(queryClient);

    await waitFor(() => expect(screen.getByText('operator')).toBeInTheDocument());
    fireEvent.click(await screen.findByRole('button', { name: 'Logout' }));

    await waitFor(() => expect(screen.getByText('Login destination')).toBeInTheDocument());
    expect(mockedAuthApi.logout).toHaveBeenCalledOnce();
    expect(queryClient.getQueryData(['jobs'])).toBeUndefined();
  });
});
