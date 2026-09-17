import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import SecurityOutlinedIcon from '@mui/icons-material/SecurityOutlined';
import { Alert, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, Stack, Typography } from '@mui/material';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import { datasourceQueryKeys, deleteDatasource, getDatasource, invalidateDatasourceQueries } from '../api/datasourcesApi';
import { useAuth } from '../auth/useAuth';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.detail : 'Unable to load this datasource.';
}

function DetailRows({ details }: { details: Array<[string, string | number | undefined | null]> }) {
  return (
    <Stack divider={<div role="separator" />}>
      {details.map(([label, value]) => (
        <Stack key={label} direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ py: 1.25 }}>
          <Typography sx={{ minWidth: { sm: 220 } }} color="text.secondary">{label}</Typography>
          <Typography sx={{ overflowWrap: 'anywhere' }}>{value ?? 'Not configured'}</Typography>
        </Stack>
      ))}
    </Stack>
  );
}

export default function DatasourceDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { user } = useAuth();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const datasourceQuery = useQuery({
    queryKey: datasourceQueryKeys.detail(id ?? ''),
    queryFn: () => getDatasource(id ?? ''),
    enabled: Boolean(id)
  });

  const deleteMutation = useMutation({
    mutationFn: (datasourceId: string) => deleteDatasource(datasourceId),
    onSuccess: () => {
      void invalidateDatasourceQueries(queryClient);
      navigate('/datasources');
    }
  });

  if (datasourceQuery.isPending) {
    return <LoadingState label="Loading datasource" />;
  }

  if (datasourceQuery.isError || !datasourceQuery.data) {
    return <Alert severity="error">{errorMessage(datasourceQuery.error)}</Alert>;
  }

  const datasource = datasourceQuery.data;
  const capabilities = datasource.capabilities;
  const technicalParams = Object.entries(datasource.technicalParams ?? {})
    .sort(([first], [second]) => first.localeCompare(second))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');

  return (
    <Stack spacing={3}>
      <PageHeader
        title={datasource.name ?? 'Datasource'}
        description="Read-only connection profile details. Secrets are never returned by the server."
        backLink={
          <Button component={RouterLink} to="/datasources" variant="text" startIcon={<ArrowBackIcon />}>
            Back to datasources
          </Button>
        }
        actions={
          <>
            {datasource.canEdit && datasource.id && (
              <Button
                component={RouterLink}
                to={`/datasources/${datasource.id}/edit`}
                variant="contained"
                startIcon={<EditOutlinedIcon />}
              >
                Edit datasource
              </Button>
            )}
            {user?.role === 'ADMIN' && datasource.id && (
              <Button
                component={RouterLink}
                to={`/datasources/${datasource.id}/permissions`}
                variant="outlined"
                startIcon={<SecurityOutlinedIcon />}
              >
                Manage permissions
              </Button>
            )}
            {user?.role === 'ADMIN' && datasource.id && (
              <Button
                color="error"
                variant="outlined"
                startIcon={<DeleteOutlineIcon />}
                onClick={() => setDeleteOpen(true)}
              >
                Delete datasource
              </Button>
            )}
          </>
        }
      />
      <SurfaceSection title="Connection">
        <DetailRows details={[
          ['Connector', datasource.connectorType],
          ['Safe connection', datasource.safeConnectDisplay],
          ['Security', datasource.securityConfigured ? 'Configured' : 'Not configured'],
          ['Created', datasource.createdAt],
          ['Updated', datasource.updatedAt]
        ]} />
      </SurfaceSection>
      <SurfaceSection title="Capabilities" description="Capabilities and supported modes are derived by the server connector registry.">
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
          {capabilities?.sourceCapable && <Chip label="Source capable" color="primary" />}
          {capabilities?.sinkCapable && <Chip label="Sink capable" color="secondary" />}
          {capabilities?.singleJobOnly && <Chip label="Single job only" variant="outlined" />}
          {(!capabilities?.sourceCapable && !capabilities?.sinkCapable) && (
            <Typography color="text.secondary">No capabilities reported.</Typography>
          )}
        </Stack>
        <DetailRows details={[
          ['Source modes', capabilities?.sourceModes?.join(', ')],
          ['Sink modes', capabilities?.sinkModes?.join(', ')],
          ['Source query support', capabilities?.sourceQuery === undefined
            ? undefined : capabilities.sourceQuery ? 'Supported' : 'Not supported']
        ]} />
      </SurfaceSection>
      <SurfaceSection title="Technical parameters" description="Only non-secret connector configuration is displayed.">
        <Typography component="pre" sx={{ m: 0, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontFamily: 'inherit' }}>
          {technicalParams || 'No technical parameters configured.'}
        </Typography>
      </SurfaceSection>
      <Dialog
        open={deleteOpen}
        onClose={() => !deleteMutation.isPending && setDeleteOpen(false)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Delete datasource</DialogTitle>
        <DialogContent>
          {deleteMutation.isError && <Alert severity="error" sx={{ mb: 2 }}>{errorMessage(deleteMutation.error)}</Alert>}
          <Typography>
            Delete {datasource.name ?? 'this datasource'}? A profile referenced by a job cannot be deleted.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteOpen(false)} disabled={deleteMutation.isPending}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={() => datasource.id && deleteMutation.mutate(datasource.id)}
            disabled={deleteMutation.isPending || !datasource.id}
            startIcon={<DeleteOutlineIcon />}
          >
            {deleteMutation.isPending ? 'Deleting...' : 'Delete datasource'}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
