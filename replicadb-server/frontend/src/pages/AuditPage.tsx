import AccessTimeOutlinedIcon from '@mui/icons-material/AccessTimeOutlined';
import CloseIcon from '@mui/icons-material/Close';
import FilterAltOutlinedIcon from '@mui/icons-material/FilterAltOutlined';
import {
  Alert,
  Box,
  Button,
  Chip,
  Divider,
  Drawer,
  IconButton,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Typography
} from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { ApiError } from '../api/client';
import {
  auditQueryKeys,
  listAuditEvents,
  type AuditAction,
  type AuditEventResponse,
  type AuditFilters,
  type AuditResourceType
} from '../api/auditApi';
import { listUsers, type UserResponse } from '../api/usersApi';
import EmptyState from '../components/EmptyState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

const actions: AuditAction[] = [
  'LOGIN_SUCCEEDED', 'LOGIN_FAILED', 'LOGOUT', 'USER_CREATED', 'USER_UPDATED',
  'USER_PASSWORD_CHANGED', 'DATASOURCE_CREATED', 'DATASOURCE_UPDATED', 'DATASOURCE_DELETED',
  'DATASOURCE_PERMISSION_REPLACED', 'DATASOURCE_PERMISSION_REVOKED', 'JOB_CREATED',
  'JOB_UPDATED', 'JOB_DELETED', 'JOB_DATASOURCE_BINDING_REPLACED', 'JOB_DATASOURCE_BINDING_ENABLED',
  'JOB_DATASOURCE_BINDING_DISABLED', 'JOB_PERMISSION_REPLACED', 'JOB_PERMISSION_REVOKED',
  'JOB_SCHEDULE_UPSERTED', 'JOB_SCHEDULE_DELETED', 'RUN_TRIGGERED', 'RUN_CANCEL_REQUESTED',
  'RUN_RETRIED', 'RUN_SUCCEEDED', 'RUN_FAILED', 'RUN_CANCELLED'
];
const resourceTypes: AuditResourceType[] = ['USER', 'DATASOURCE', 'JOB_DEFINITION', 'JOB_RUN', 'SESSION'];
const systemActors = ['system:scheduler', 'system:bootstrap', 'system:api'];

function label(value: string): string {
  return value.toLowerCase().replace(/_/g, ' ').replace(/\b\w/g, (character: string) => character.toUpperCase());
}

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to load audit events.';
}

function formatDate(value?: string): string {
  if (!value) return 'Not available';
  return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value));
}

function resourceLabel(event: AuditEventResponse): string {
  return event.resourceId ? `${label(event.resourceType ?? 'RESOURCE')} · ${event.resourceId}` : label(event.resourceType ?? 'RESOURCE');
}

function DetailRows({ event }: { event: AuditEventResponse }) {
  const rows: Array<[string, string]> = [
    ['Event ID', event.id ?? 'Not available'],
    ['Occurred', formatDate(event.occurredAt)],
    ['Actor', event.actorUsername ?? 'Not available'],
    ['Actor ID', event.actorUserId ?? 'System or anonymous actor'],
    ['Source address', event.sourceAddress ?? 'Not available'],
    ['Action', label(event.action ?? 'UNKNOWN')],
    ['Resource', resourceLabel(event)],
    ['Outcome', label(event.outcome ?? 'UNKNOWN')]
  ];
  return (
    <Stack divider={<Divider />}>
      {rows.map(([name, value]) => (
        <Stack key={name} direction={{ xs: 'column', sm: 'row' }} spacing={1} sx={{ py: 1.25 }}>
          <Typography color="text.secondary" sx={{ minWidth: { sm: 140 } }}>{name}</Typography>
          <Typography sx={{ overflowWrap: 'anywhere' }}>{value}</Typography>
        </Stack>
      ))}
    </Stack>
  );
}

export default function AuditPage() {
  const [page, setPage] = useState(0);
  const [filters, setFilters] = useState<AuditFilters>({});
  const [draftFilters, setDraftFilters] = useState<AuditFilters>({});
  const [selectedEvent, setSelectedEvent] = useState<AuditEventResponse>();
  const size = 25;
  const eventsQuery = useQuery({
    queryKey: auditQueryKeys.list(page, size, filters),
    queryFn: () => listAuditEvents(page, size, filters)
  });
  const usersQuery = useQuery({ queryKey: ['users', 'audit-actors'], queryFn: () => listUsers(0, 100) });

  const users = (usersQuery.data?.content ?? []).filter((user): user is UserResponse & { id: string } => Boolean(user.id));
  const events = eventsQuery.data?.content ?? [];
  const totalElements = eventsQuery.data?.totalElements ?? 0;

  const applyFilters = () => {
    setPage(0);
    setFilters(Object.fromEntries(Object.entries(draftFilters).filter(([, value]) => value)) as AuditFilters);
  };
  const clearFilters = () => {
    setDraftFilters({});
    setFilters({});
    setPage(0);
  };

  if (eventsQuery.isPending) return <LoadingState label="Loading audit events" />;
  if (eventsQuery.isError) return <Alert severity="error">{errorMessage(eventsQuery.error)}</Alert>;

  return (
    <Stack spacing={3}>
      <PageHeader
        title="Audit"
        description="Historical activity across access, configuration, permissions, and replication runs."
      />
      <SurfaceSection
        title="Search the ledger"
        description="Use the filters to narrow the record, then open an event for its full context."
        actions={<Button variant="outlined" startIcon={<FilterAltOutlinedIcon />} onClick={applyFilters}>Apply filters</Button>}
      >
        <Stack spacing={2}>
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
            <TextField
              label="From"
              type="date"
              value={draftFilters.from?.slice(0, 10) ?? ''}
              onChange={event => setDraftFilters(current => ({ ...current, from: event.target.value ? `${event.target.value}T00:00:00Z` : undefined }))}
              InputLabelProps={{ shrink: true }}
              sx={{ flex: 1 }}
            />
            <TextField
              label="To"
              type="date"
              value={draftFilters.to?.slice(0, 10) ?? ''}
              onChange={event => setDraftFilters(current => ({ ...current, to: event.target.value ? `${event.target.value}T23:59:59Z` : undefined }))}
              InputLabelProps={{ shrink: true }}
              sx={{ flex: 1 }}
            />
            <TextField select label="Action" value={draftFilters.action ?? ''} onChange={event => setDraftFilters(current => ({ ...current, action: (event.target.value || undefined) as AuditAction | undefined }))} sx={{ flex: 1 }}>
              <MenuItem value="">All actions</MenuItem>
              {actions.map(action => <MenuItem key={action} value={action}>{label(action)}</MenuItem>)}
            </TextField>
          </Stack>
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
            <TextField select label="Resource type" value={draftFilters.resourceType ?? ''} onChange={event => setDraftFilters(current => ({ ...current, resourceType: (event.target.value || undefined) as AuditResourceType | undefined }))} sx={{ flex: 1 }}>
              <MenuItem value="">All resource types</MenuItem>
              {resourceTypes.map(type => <MenuItem key={type} value={type}>{label(type)}</MenuItem>)}
            </TextField>
            <TextField label="Resource ID" value={draftFilters.resourceId ?? ''} onChange={event => setDraftFilters(current => ({ ...current, resourceId: event.target.value || undefined }))} sx={{ flex: 1 }} />
            <TextField select label="Actor" value={draftFilters.actorUserId ?? ''} onChange={event => setDraftFilters(current => ({ ...current, actorUserId: event.target.value || undefined }))} sx={{ flex: 1 }}>
              <MenuItem value="">All actors</MenuItem>
              {users.map(user => <MenuItem key={user.id} value={user.id}>{user.username ?? user.id}</MenuItem>)}
              <Divider />
              {systemActors.map(actor => <MenuItem key={actor} value={actor} disabled>{actor} (system actor)</MenuItem>)}
            </TextField>
          </Stack>
          <Stack direction="row" justifyContent="space-between" alignItems="center">
            <Typography variant="body2" color="text.secondary">{totalElements} event{totalElements === 1 ? '' : 's'} found</Typography>
            <Button variant="text" onClick={clearFilters} disabled={Object.keys(filters).length === 0 && Object.keys(draftFilters).length === 0}>Clear filters</Button>
          </Stack>
        </Stack>
      </SurfaceSection>
      <SurfaceSection title="Audit events" description="Newest events appear first.">
        {events.length === 0 ? <EmptyState title="No audit events found." description="Try widening the date range or clearing a filter." /> : (
          <>
            <TableContainer sx={{ overflowX: 'auto' }}>
              <Table aria-label="Audit events" sx={{ minWidth: 850 }}>
                <TableHead><TableRow><TableCell>Occurred</TableCell><TableCell>Actor</TableCell><TableCell>Action</TableCell><TableCell>Resource</TableCell><TableCell>Outcome</TableCell><TableCell>Source</TableCell></TableRow></TableHead>
                <TableBody>
                  {events.map(event => <TableRow key={event.id} hover tabIndex={0} onClick={() => setSelectedEvent(event)} onKeyDown={keyboardEvent => { if (keyboardEvent.key === 'Enter' || keyboardEvent.key === ' ') { keyboardEvent.preventDefault(); setSelectedEvent(event); } }} sx={{ cursor: 'pointer' }}>
                    <TableCell><Stack direction="row" spacing={1} alignItems="center"><AccessTimeOutlinedIcon fontSize="small" color="action" /><Typography variant="body2">{formatDate(event.occurredAt)}</Typography></Stack></TableCell>
                    <TableCell>{event.actorUsername ?? 'Not available'}</TableCell>
                    <TableCell>{label(event.action ?? 'UNKNOWN')}</TableCell>
                    <TableCell sx={{ maxWidth: 240, overflowWrap: 'anywhere' }}>{resourceLabel(event)}</TableCell>
                    <TableCell><Chip label={label(event.outcome ?? 'UNKNOWN')} size="small" color={event.outcome === 'SUCCESS' ? 'success' : event.outcome === 'FAILURE' ? 'error' : 'default'} /></TableCell>
                    <TableCell>{event.sourceAddress ?? 'System'}</TableCell>
                  </TableRow>)}
                </TableBody>
              </Table>
            </TableContainer>
            <TablePagination component="div" count={totalElements} page={page} onPageChange={(_, nextPage) => setPage(nextPage)} rowsPerPage={size} rowsPerPageOptions={[size]} />
          </>
        )}
      </SurfaceSection>
      <Drawer anchor="right" open={Boolean(selectedEvent)} onClose={() => setSelectedEvent(undefined)} PaperProps={{ sx: { width: { xs: '100%', sm: 480 }, p: { xs: 2, sm: 3 } } }}>
        {selectedEvent && <Stack spacing={2} component="aside" aria-label="Audit event detail">
          <Stack direction="row" justifyContent="space-between" alignItems="flex-start"><Box><Typography variant="h5" component="h2">Event detail</Typography><Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>Full audit context</Typography></Box><IconButton aria-label="Close event detail" onClick={() => setSelectedEvent(undefined)}><CloseIcon /></IconButton></Stack>
          <DetailRows event={selectedEvent} />
          <Box><Typography variant="subtitle2" gutterBottom>Context</Typography><Box component="pre" sx={{ m: 0, p: 1.5, bgcolor: 'action.hover', borderRadius: 1, overflow: 'auto', whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontFamily: 'monospace', fontSize: '0.8rem' }}>{JSON.stringify(selectedEvent.detail ?? {}, null, 2)}</Box></Box>
        </Stack>}
      </Drawer>
    </Stack>
  );
}
