import { Alert, Stack, Typography } from '@mui/material';
import EmptyState from './EmptyState';
import LoadingState from './LoadingState';
import type { RunLogResponse } from '../api/runsApi';

type RunLogViewerProps = {
  log?: RunLogResponse;
  loading?: boolean;
  error?: boolean;
};

export default function RunLogViewer({ log, loading = false, error = false }: RunLogViewerProps) {
  if (loading) {
    return <LoadingState label="Loading run log" compact />;
  }
  if (error) {
    return <Alert severity="error">Unable to load the run log.</Alert>;
  }
  if (!log?.content) {
    return <EmptyState title="No detailed log available." />;
  }

  return (
    <Stack spacing={1.5}>
      {log.truncated && <Alert severity="warning">This log was truncated to 256 KiB.</Alert>}
      <Typography color="text.secondary" variant="body2">
        Captured {log.capturedSize ?? 0} bytes{log.formatVersion ? ` · format ${log.formatVersion}` : ''}
      </Typography>
      <Typography
        component="pre"
        sx={{
          maxWidth: '100%',
          overflowX: 'auto',
          whiteSpace: 'pre-wrap',
          overflowWrap: 'anywhere',
          fontFamily: 'monospace',
          m: 0
        }}
      >
        {log.content}
      </Typography>
    </Stack>
  );
}
