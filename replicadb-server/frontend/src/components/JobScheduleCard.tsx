import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { ApiError } from '../api/client';
import {
  deleteSchedule,
  getSchedule,
  upsertSchedule
} from '../api/scheduleApi';
import type { components } from '../api/schema';
import {
  buildCronExpression,
  createDefaultCronBuilder,
  cronModeOptions,
  dayOfWeekOptions,
  getTimeZoneOption,
  parseCronExpression,
  timeZoneLabel,
  timeZoneOptions,
  type CronBuilder,
  type CronMode
} from '../utils/cronSchedule';
import EmptyState from './EmptyState';
import LoadingState from './LoadingState';
import SurfaceSection from './SurfaceSection';

type ScheduleForm = components['schemas']['JobScheduleRequest'];
type ScheduleResponse = components['schemas']['JobScheduleResponse'];

const emptySchedule: ScheduleForm = {
  cronExpression: '',
  timeZone: 'UTC',
  enabled: true
};

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to update the schedule.';
}

export default function JobScheduleCard({ jobId }: { jobId: string }) {
  const queryClient = useQueryClient();
  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [form, setForm] = useState<ScheduleForm>(emptySchedule);
  const [cronBuilder, setCronBuilder] = useState<CronBuilder>(createDefaultCronBuilder);
  const [validationError, setValidationError] = useState<string>();
  const [editorError, setEditorError] = useState<string>();
  const [deleteError, setDeleteError] = useState<string>();

  const scheduleQuery = useQuery({
    queryKey: ['schedule', jobId],
    queryFn: () => getSchedule(jobId),
    enabled: Boolean(jobId)
  });

  const scheduleMutation = useMutation({
    mutationFn: (input: ScheduleForm) => upsertSchedule(jobId, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['schedule', jobId] });
      setEditorOpen(false);
      setEditorError(undefined);
    },
    onError: error => {
      setEditorError(errorMessage(error));
    }
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteSchedule(jobId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['schedule', jobId] });
      setDeleteOpen(false);
      setDeleteError(undefined);
    },
    onError: error => {
      setDeleteError(errorMessage(error));
    }
  });

  const openEditor = (schedule?: ScheduleResponse | null) => {
    setEditing(Boolean(schedule));
    setCronBuilder(schedule?.cronExpression ? parseCronExpression(schedule.cronExpression) : createDefaultCronBuilder());
    setForm({
      cronExpression: schedule?.cronExpression ?? '',
      timeZone: schedule?.timeZone ?? 'UTC',
      enabled: schedule?.enabled ?? true
    });
    setValidationError(undefined);
    setEditorError(undefined);
    setEditorOpen(true);
  };

  const submitEditor = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const cronResult = buildCronExpression(cronBuilder);
    if (cronResult.error || !cronResult.expression) {
      setValidationError(cronResult.error ?? 'Cron expression is required.');
      return;
    }
    setValidationError(undefined);
    scheduleMutation.mutate({ ...form, cronExpression: cronResult.expression });
  };

  const updateCronBuilder = (changes: Partial<CronBuilder>) => {
    setCronBuilder(current => ({ ...current, ...changes }));
    setValidationError(undefined);
  };

  const cronResult = buildCronExpression(cronBuilder);
  const cronError = validationError ?? cronResult.error;
  const selectedTimeZone = getTimeZoneOption(form.timeZone);

  if (scheduleQuery.isPending) {
    return <LoadingState label="Loading schedule" compact />;
  }

  if (scheduleQuery.isError) {
    return <Alert severity="error">Unable to load the recurring schedule.</Alert>;
  }

  const schedule = scheduleQuery.data;

  return (
    <>
      <SurfaceSection
        title="Recurring schedule"
        actions={schedule ? (
          <>
            <Button variant="outlined" onClick={() => openEditor(schedule)}>Edit</Button>
            <Button color="error" onClick={() => {
              setDeleteError(undefined);
              setDeleteOpen(true);
            }}>Delete</Button>
          </>
        ) : undefined}
      >
        {schedule ? (
          <Stack spacing={1}>
            <Typography>CRON expression: {schedule.cronExpression ?? 'Not configured'}</Typography>
            <Typography>Time zone: {schedule.timeZone ?? 'UTC'}</Typography>
            <Typography>Enabled: {schedule.enabled ? 'Yes' : 'No'}</Typography>
            <Typography>Next fire time: {schedule.nextFireTime ?? 'Not scheduled'}</Typography>
          </Stack>
        ) : (
          <EmptyState title="No recurring schedule configured" action={<Button variant="outlined" onClick={() => openEditor()}>Create schedule</Button>} />
        )}
      </SurfaceSection>

      <Dialog open={editorOpen} onClose={() => !scheduleMutation.isPending && setEditorOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>{editing ? 'Edit schedule' : 'Create schedule'}</DialogTitle>
        <DialogContent>
          <Stack component="form" id="schedule-form" onSubmit={submitEditor} spacing={2} sx={{ pt: 1 }}>
            {editorError && <Alert severity="error">{editorError}</Alert>}
            <Typography variant="body2" color="text.secondary">
              Build a Quartz CRON schedule from the fields below. Seconds are set to zero automatically.
            </Typography>
            <TextField
              select
              label="Schedule frequency"
              value={cronBuilder.mode}
              onChange={event => updateCronBuilder({ mode: event.target.value as CronMode })}
              fullWidth
            >
              {cronModeOptions.map(option => <MenuItem key={option.value} value={option.value}>{option.label}</MenuItem>)}
            </TextField>
            {cronBuilder.mode === 'everyMinutes' && (
              <TextField
                label="Run every (minutes)"
                type="number"
                value={cronBuilder.everyMinutes}
                onChange={event => updateCronBuilder({ everyMinutes: event.target.value })}
                inputProps={{ min: 1, max: 59, step: 1 }}
                helperText="Choose a whole number from 1 to 59."
                fullWidth
              />
            )}
            {cronBuilder.mode === 'hourly' && (
              <TextField
                label="At minute"
                type="number"
                value={cronBuilder.minute}
                onChange={event => updateCronBuilder({ minute: event.target.value })}
                inputProps={{ min: 0, max: 59, step: 1 }}
                helperText="Choose a minute from 0 to 59."
                fullWidth
              />
            )}
            {(cronBuilder.mode === 'daily' || cronBuilder.mode === 'weekly' || cronBuilder.mode === 'monthly') && (
              <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
                <TextField
                  label="Hour"
                  type="number"
                  value={cronBuilder.hour}
                  onChange={event => updateCronBuilder({ hour: event.target.value })}
                  inputProps={{ min: 0, max: 23, step: 1 }}
                  helperText="0 to 23"
                  fullWidth
                />
                <TextField
                  label="Minute"
                  type="number"
                  value={cronBuilder.minute}
                  onChange={event => updateCronBuilder({ minute: event.target.value })}
                  inputProps={{ min: 0, max: 59, step: 1 }}
                  helperText="0 to 59"
                  fullWidth
                />
              </Box>
            )}
            {cronBuilder.mode === 'weekly' && (
              <TextField
                select
                label="Day of week"
                value={cronBuilder.dayOfWeek}
                onChange={event => updateCronBuilder({ dayOfWeek: event.target.value })}
                fullWidth
              >
                {dayOfWeekOptions.map(option => <MenuItem key={option.value} value={option.value}>{option.label}</MenuItem>)}
              </TextField>
            )}
            {cronBuilder.mode === 'monthly' && (
              <TextField
                label="Day of month"
                type="number"
                value={cronBuilder.dayOfMonth}
                onChange={event => updateCronBuilder({ dayOfMonth: event.target.value })}
                inputProps={{ min: 1, max: 31, step: 1 }}
                helperText="Choose a day from 1 to 31."
                fullWidth
              />
            )}
            <TextField
              label="CRON expression"
              value={cronBuilder.mode === 'custom' ? cronBuilder.customExpression : cronResult.expression ?? ''}
              onChange={event => cronBuilder.mode === 'custom' && updateCronBuilder({ customExpression: event.target.value })}
              error={Boolean(cronError)}
              helperText={cronError ?? 'Quartz format: seconds minutes hours day month weekday.'}
              required
              fullWidth
              InputProps={{ readOnly: cronBuilder.mode !== 'custom' }}
            />
            <Autocomplete
              options={timeZoneOptions}
              value={selectedTimeZone}
              onChange={(_, option) => setForm(current => ({ ...current, timeZone: option?.value ?? 'UTC' }))}
              getOptionLabel={timeZoneLabel}
              isOptionEqualToValue={(option, value) => option.value === value.value}
              renderInput={params => (
                <TextField
                  {...params}
                  label="Time zone"
                  helperText="Standard UTC offset; daylight-saving changes are handled by the scheduler."
                  required
                />
              )}
              fullWidth
              autoHighlight
            />
            <FormControlLabel
              control={
                <Switch
                  checked={form.enabled ?? true}
                  onChange={event => setForm(current => ({ ...current, enabled: event.target.checked }))}
                />
              }
              label="Enabled"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditorOpen(false)}>Cancel</Button>
          <Button type="submit" form="schedule-form" variant="contained" disabled={scheduleMutation.isPending}>
            {scheduleMutation.isPending ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteOpen} onClose={() => !deleteMutation.isPending && setDeleteOpen(false)}>
        <DialogTitle>Remove schedule</DialogTitle>
        <DialogContent>
          {deleteError && <Alert severity="error">{deleteError}</Alert>}
          <DialogContentText>
            Remove the recurring schedule for this job? This cannot be undone from here.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteOpen(false)}>Cancel</Button>
          <Button color="error" variant="contained" onClick={() => deleteMutation.mutate()} disabled={deleteMutation.isPending}>
            {deleteMutation.isPending ? 'Removing...' : 'Remove'}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
