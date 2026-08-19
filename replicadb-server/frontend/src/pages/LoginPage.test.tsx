import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as authApi from '../api/authApi';
import { AuthProvider } from '../auth/AuthContext';
import { theme } from '../theme/theme';
import LoginPage from './LoginPage';

vi.mock('../api/authApi', () => ({
  getMe: vi.fn(),
  login: vi.fn(),
  logout: vi.fn()
}));

const mockedAuthApi = vi.mocked(authApi);

function renderLogin() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  });
  const router = createMemoryRouter([
    { path: '/login', element: <LoginPage /> },
    { path: '/', element: <div>Dashboard destination</div> }
  ], { initialEntries: ['/login'] });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('LoginPage', () => {
  beforeEach(() => {
    mockedAuthApi.getMe.mockRejectedValue(new ApiError(401, 'Unauthorized', 'Authentication required'));
  });

  it('logs in and navigates to the dashboard', async () => {
    mockedAuthApi.login.mockResolvedValue({ id: 'user-id', username: 'operator', role: 'OPERATOR' });

    renderLogin();
    expect(screen.getByRole('link', { name: 'ReplicaDB' })).toHaveAttribute('href', '/');
    expect(screen.getByRole('form', { name: 'Sign-in form' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'operator' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'password' } });
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));

    await waitFor(() => expect(screen.getByText('Dashboard destination')).toBeInTheDocument());
    expect(mockedAuthApi.login).toHaveBeenCalledWith('operator', 'password');
  });

  it('renders the API error and remains on the login page', async () => {
    mockedAuthApi.login.mockRejectedValue(new ApiError(401, 'Unauthorized', 'Invalid credentials'));

    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'operator' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'wrong' } });
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Invalid credentials');
    expect(screen.getByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();
  });

  it('keeps submit disabled until both fields contain values', () => {
    renderLogin();
    const submit = screen.getByRole('button', { name: 'Sign in' });

    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'operator' } });
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'password' } });
    expect(submit).toBeEnabled();
  });
});
