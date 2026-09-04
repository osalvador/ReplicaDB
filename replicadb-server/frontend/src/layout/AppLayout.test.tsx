import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as authApi from '../api/authApi';
import { AuthProvider } from '../auth/AuthContext';
import { theme } from '../theme/theme';
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
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('AppLayout', () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockedAuthApi.getMe.mockResolvedValue({ id: 'user-id', username: 'operator', role: 'OPERATOR' });
    mockedAuthApi.logout.mockResolvedValue(undefined);
  });

  it('logs out, clears cached data, and navigates to login', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } }
    });
    queryClient.setQueryData(['jobs'], { content: ['private data'] });
    renderLayout(queryClient);

    await waitFor(() => expect(screen.getAllByText('operator').length).toBeGreaterThan(0));
    expect(screen.getAllByRole('link', { name: 'ReplicaDB' })[0]).toHaveAttribute('href', '/');
    expect(screen.getByRole('link', { name: 'Jobs' })).toHaveAttribute('href', '/jobs');
    expect(screen.getByRole('link', { name: 'Dashboard' })).toHaveAttribute('href', '/');
    expect(screen.getByRole('link', { name: 'Datasources' })).toHaveAttribute('href', '/datasources');
    expect(screen.getByRole('group', { name: 'Signed-in identity' })).toHaveTextContent('OPERATOR');
    expect(screen.getByRole('main')).toHaveTextContent('Dashboard content');
    fireEvent.click(await screen.findByRole('button', { name: 'Logout' }));

    await waitFor(() => expect(screen.getByText('Login destination')).toBeInTheDocument());
    expect(mockedAuthApi.logout).toHaveBeenCalledOnce();
    expect(queryClient.getQueryData(['jobs'])).toBeUndefined();
  });

  it('shows the users link for an admin', async () => {
    mockedAuthApi.getMe.mockResolvedValue({ id: 'admin-id', username: 'admin', role: 'ADMIN' });
    renderLayout(new QueryClient({ defaultOptions: { queries: { retry: false } } }));

    expect(await screen.findByRole('link', { name: 'Users' })).toHaveAttribute('href', '/users');
  });

  it('collapses the desktop navigation to icon-only links', async () => {
    const { container } = renderLayout(new QueryClient({ defaultOptions: { queries: { retry: false } } }));

    await screen.findAllByText('operator');
    fireEvent.click(screen.getByRole('button', { name: 'Collapse navigation' }));

    expect(window.localStorage.getItem('replicadb.navigation.collapsed')).toBe('true');
    expect(screen.getByRole('button', { name: 'Expand navigation' })).toBeInTheDocument();
    const desktopDrawer = container.querySelector<HTMLElement>('.MuiDrawer-docked');
    if (!desktopDrawer) {
      throw new Error('Permanent desktop drawer not found');
    }
    expect(within(desktopDrawer).queryByText('Control plane')).not.toBeInTheDocument();
    expect(within(desktopDrawer).getByRole('link', { name: 'Jobs' })).toHaveAttribute('href', '/jobs');

    fireEvent.click(screen.getByRole('button', { name: 'Expand navigation' }));
    expect(window.localStorage.getItem('replicadb.navigation.collapsed')).toBe('false');
    expect(within(desktopDrawer).getByText('Control plane')).toBeInTheDocument();
  });

  it('restores the collapsed desktop navigation after remounting', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const firstRender = renderLayout(queryClient);

    await screen.findAllByText('operator');
    fireEvent.click(screen.getByRole('button', { name: 'Collapse navigation' }));
    firstRender.unmount();

    renderLayout(queryClient);

    await screen.findAllByText('operator');
    expect(screen.getByRole('button', { name: 'Expand navigation' })).toBeInTheDocument();
    const desktopDrawer = document.querySelector<HTMLElement>('.MuiDrawer-docked');
    if (!desktopDrawer) {
      throw new Error('Permanent desktop drawer not found');
    }
    expect(within(desktopDrawer).queryByText('Control plane')).not.toBeInTheDocument();
  });

  it.each(['OPERATOR', 'VIEWER'] as const)('hides the users link for a %s user', async role => {
    mockedAuthApi.getMe.mockResolvedValue({ id: 'user-id', username: role.toLowerCase(), role });
    renderLayout(new QueryClient({ defaultOptions: { queries: { retry: false } } }));

    await screen.findAllByText(role.toLowerCase());
    expect(screen.queryByRole('link', { name: 'Users' })).not.toBeInTheDocument();
  });
});
