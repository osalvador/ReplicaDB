import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import * as scheduleApi from '../api/scheduleApi';
import JobScheduleCard from './JobScheduleCard';
import { theme } from '../theme/theme';

vi.mock('../api/scheduleApi', () => ({
  deleteSchedule: vi.fn(),
  getSchedule: vi.fn(),
  upsertSchedule: vi.fn()
}));

const mockedScheduleApi = vi.mocked(scheduleApi);

const existingSchedule = {
  jobDefinitionId: 'job-1',
  cronExpression: '0 0 * * * ?',
  timeZone: 'UTC',
  enabled: true,
  nextFireTime: '2026-08-18T17:00:00Z'
};

function renderCard() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <JobScheduleCard jobId="job-1" />
        </MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

function selectOption(label: string, option: string) {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: label }));
  fireEvent.click(screen.getByRole('option', { name: option }));
}

describe('JobScheduleCard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the empty state when no schedule is configured', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(null);

    renderCard();

    expect(await screen.findByText('No recurring schedule configured')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Recurring schedule' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Create schedule' })).toBeInTheDocument();
  });

  it('renders the configured schedule details', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(existingSchedule);

    renderCard();

    expect(await screen.findByText(/CRON expression:\s+0 0 \* \* \* \?/)).toBeInTheDocument();
    expect(screen.getByText(/Time zone:\s+UTC/)).toBeInTheDocument();
    expect(screen.getByText(/Next fire time:\s+2026-08-18T17:00:00Z/)).toBeInTheDocument();
  });

  it('submits an entered create schedule and closes the dialog', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(null);
    mockedScheduleApi.upsertSchedule.mockResolvedValue(existingSchedule);

    renderCard();
    fireEvent.click(await screen.findByRole('button', { name: 'Create schedule' }));
    selectOption('Schedule frequency', 'Every day');
    fireEvent.change(screen.getByLabelText('Hour'), { target: { value: '2' } });
    fireEvent.change(screen.getByLabelText('Minute'), { target: { value: '15' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Time zone' }), { target: { value: 'Europe/Madrid' } });
    fireEvent.click(await screen.findByText('UTC+01:00 · Europe/Madrid'));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Enabled' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(mockedScheduleApi.upsertSchedule).toHaveBeenCalledWith('job-1', {
      cronExpression: '0 15 2 * * ?',
      timeZone: 'Europe/Madrid',
      enabled: false
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('confirms deletion with the required warning text', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(existingSchedule);
    mockedScheduleApi.deleteSchedule.mockResolvedValue();

    renderCard();
    fireEvent.click(await screen.findByRole('button', { name: 'Delete' }));
    expect(screen.getByText('Remove the recurring schedule for this job? This cannot be undone from here.'))
      .toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }));

    await waitFor(() => expect(mockedScheduleApi.deleteSchedule).toHaveBeenCalledWith('job-1'));
  });

  it('keeps the editor open and shows an upsert error', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(null);
    mockedScheduleApi.upsertSchedule.mockRejectedValue(
      new ApiError(400, 'Invalid schedule', 'The cron expression is invalid.')
    );

    renderCard();
    fireEvent.click(await screen.findByRole('button', { name: 'Create schedule' }));
    selectOption('Schedule frequency', 'Advanced CRON expression');
    fireEvent.change(screen.getByRole('textbox', { name: 'CRON expression' }), { target: { value: '0 0 * * * ?' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(await screen.findByText('The cron expression is invalid.')).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('The cron expression is invalid.');
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(mockedScheduleApi.deleteSchedule).not.toHaveBeenCalled();
  });

  it('blocks an invalid guided value before submitting', async () => {
    mockedScheduleApi.getSchedule.mockResolvedValue(null);

    renderCard();
    fireEvent.click(await screen.findByRole('button', { name: 'Create schedule' }));
    selectOption('Schedule frequency', 'Every few minutes');
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Run every (minutes)' }), { target: { value: '0' } });

    expect(screen.getByText('Interval must be a whole number between 1 and 59.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockedScheduleApi.upsertSchedule).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
