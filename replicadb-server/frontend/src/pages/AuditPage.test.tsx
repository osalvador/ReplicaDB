import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as auditApi from '../api/auditApi';
import * as usersApi from '../api/usersApi';
import { theme } from '../theme/theme';
import AuditPage from './AuditPage';

vi.mock('../api/auditApi', async () => ({
  ...(await vi.importActual<typeof import('../api/auditApi')>('../api/auditApi')),
  listAuditEvents: vi.fn()
}));
vi.mock('../api/usersApi', async () => ({
  ...(await vi.importActual<typeof import('../api/usersApi')>('../api/usersApi')),
  listUsers: vi.fn()
}));

const mockedAuditApi = vi.mocked(auditApi);
const mockedUsersApi = vi.mocked(usersApi);

function renderPage() {
  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter><AuditPage /></MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe('AuditPage', () => {
  beforeEach(() => {
    mockedAuditApi.listAuditEvents.mockResolvedValue({
      content: [{
        id: 'event-1',
        occurredAt: '2026-09-04T10:00:00Z',
        actorUserId: 'user-1',
        actorUsername: 'admin',
        sourceAddress: '127.0.0.1',
        action: 'JOB_UPDATED',
        resourceType: 'JOB_DEFINITION',
        resourceId: 'job-1',
        outcome: 'SUCCESS',
        detail: { name: 'Orders replication' }
      }],
      page: 0,
      size: 25,
      totalElements: 1
    });
    mockedUsersApi.listUsers.mockResolvedValue({ content: [{ id: 'user-1', username: 'admin', role: 'ADMIN' }], page: 0, size: 100, totalElements: 1 });
  });

  it('shows human and fixed system actors in the actor selector', async () => {
    renderPage();

    expect(await screen.findByRole('heading', { name: 'Audit' })).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByLabelText('Actor'));
    expect(await screen.findByRole('option', { name: 'admin' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: /system:scheduler/ })).toHaveAttribute('aria-disabled', 'true');
  });

  it('opens event detail from a table row', async () => {
    renderPage();

    fireEvent.click(await screen.findByRole('row', { name: /admin.*Job Updated/i }));

    expect(await screen.findByRole('heading', { name: 'Event detail' })).toBeInTheDocument();
    expect(screen.getByText(/Orders replication/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Close event detail' }));
    await waitFor(() => expect(screen.queryByRole('heading', { name: 'Event detail' })).not.toBeInTheDocument());
  });
});
