import RefreshIcon from '@mui/icons-material/Refresh';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { Alert, Box, Button, Chip, Stack, Typography } from '@mui/material';
import { useEffect, useState } from 'react';
import EmptyState from './EmptyState';
import LoadingState from './LoadingState';
import type { RunLogResponse } from '../api/runsApi';

const TRUNCATION_MARKER = '[TRUNCATED: middle omitted]';

type RunLogViewerProps = {
  log?: RunLogResponse;
  loading?: boolean;
  error?: boolean;
  onRetry?: () => void;
  retrying?: boolean;
};

function formatInstant(value: string | null | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  const instant = new Date(value);
  if (Number.isNaN(instant.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(instant);
}

function splitTruncatedLog(content: string): { beginning: string; ending: string } | undefined {
  const markerIndex = content.indexOf(TRUNCATION_MARKER);
  if (markerIndex < 0) {
    return undefined;
  }
  return {
    beginning: content.slice(0, markerIndex),
    ending: content.slice(markerIndex + TRUNCATION_MARKER.length)
  };
}

function LogBlock({ label, content }: { label: string; content: string }) {
  return (
    <Box
      component="section"
      aria-label={label}
      sx={{
        border: '1px solid rgba(80, 98, 93, 0.16)',
        borderRadius: 1,
        backgroundColor: 'background.default',
        overflow: 'hidden'
      }}
    >
      <Typography
        component="h3"
        variant="caption"
        sx={{
          display: 'block',
          px: 1.5,
          py: 0.75,
          color: 'text.secondary',
          fontWeight: 700,
          letterSpacing: '0.04em',
          textTransform: 'uppercase',
          borderBottom: '1px solid rgba(80, 98, 93, 0.12)'
        }}
      >
        {label}
      </Typography>
      <Typography
        component="pre"
        aria-label={`${label} content`}
        tabIndex={0}
        sx={{
          maxWidth: '100%',
          overflowX: 'auto',
          whiteSpace: 'pre',
          fontFamily: 'monospace',
          fontSize: '0.875rem',
          lineHeight: 1.55,
          color: 'text.primary',
          m: 0,
          p: 1.5
        }}
      >
        {content}
      </Typography>
    </Box>
  );
}

function TruncationDivider() {
  return (
    <Box
      role="separator"
      aria-label="Middle of log omitted by server limit"
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 1,
        py: 0.5,
        color: 'warning.dark',
        '&::before, &::after': {
          content: '""',
          height: '1px',
          flex: 1,
          backgroundColor: 'rgba(138, 75, 8, 0.35)'
        }
      }}
    >
      <Typography variant="caption" fontWeight={700} textAlign="center">
        Middle omitted by server limit
      </Typography>
    </Box>
  );
}

export default function RunLogViewer({
  log,
  loading = false,
  error = false,
  onRetry,
  retrying = false
}: RunLogViewerProps) {
  const [copyFeedback, setCopyFeedback] = useState<'idle' | 'copied' | 'unavailable'>('idle');

  useEffect(() => {
    setCopyFeedback('idle');
  }, [log?.runId]);

  if (loading) {
    return <LoadingState label="Loading run log" compact />;
  }
  if (error) {
    return (
      <Alert
        severity="error"
        action={onRetry ? (
          <Button
            color="inherit"
            size="small"
            startIcon={<RefreshIcon />}
            onClick={onRetry}
            disabled={retrying}
          >
            {retrying ? 'Retrying...' : 'Try again'}
          </Button>
        ) : undefined}
      >
        Unable to load the run log. Check the server connection and try again.
      </Alert>
    );
  }
  if (!log?.content) {
    return (
      <EmptyState
        title="No log output was captured for this run."
        description="The server returned an empty log."
      />
    );
  }

  const content = log.content;
  const truncatedLog = log.truncated ? splitTruncatedLog(content) : undefined;
  const capturedAt = formatInstant(log.capturedAt);

  const copyAvailableLog = async () => {
    setCopyFeedback('idle');
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback('unavailable');
      return;
    }
    try {
      await navigator.clipboard.writeText(content);
      setCopyFeedback('copied');
    } catch {
      setCopyFeedback('unavailable');
    }
  };

  return (
    <Stack spacing={1.5}>
      <Stack
        role="group"
        aria-label="Run log metadata"
        direction={{ xs: 'column', sm: 'row' }}
        spacing={{ xs: 1, sm: 1.5 }}
        alignItems={{ xs: 'flex-start', sm: 'center' }}
        flexWrap="wrap"
      >
        <Chip
          role="status"
          aria-label={log.truncated ? 'Partial log' : 'Complete log'}
          label={log.truncated ? 'Partial log' : 'Complete log'}
          color={log.truncated ? 'warning' : 'success'}
          variant="outlined"
          size="small"
        />
        <Typography color="text.secondary" variant="body2">
          {capturedAt ? <><span>Captured at </span><time dateTime={log.capturedAt ?? undefined}>{capturedAt}</time></> : 'Captured log'}
        </Typography>
      </Stack>
      {log.truncated && (
        <Alert severity="warning">
          <Typography variant="body2" fontWeight={700}>
            Only part of this log is available.
          </Typography>
          <Typography variant="body2">
            {truncatedLog
              ? 'The server kept the beginning and end of the output; the middle was omitted at the 256 KiB limit.'
              : 'The server marked this log as partial, but did not provide the truncation boundary. All returned content is shown.'}
          </Typography>
        </Alert>
      )}
      <Stack spacing={1}>
        <Button
          type="button"
          variant="outlined"
          startIcon={<ContentCopyIcon />}
          onClick={() => void copyAvailableLog()}
          sx={{ alignSelf: { xs: 'stretch', sm: 'flex-start' } }}
        >
          Copy available log
        </Button>
        {copyFeedback !== 'idle' && (
          <Typography
            role="status"
            aria-label="Log action status"
            aria-live="polite"
            color={copyFeedback === 'unavailable' ? 'error.main' : 'text.secondary'}
            variant="body2"
          >
            {copyFeedback === 'copied'
              ? 'Log copied to clipboard.'
              : 'Clipboard is unavailable. Select the log text to copy it manually.'}
          </Typography>
        )}
      </Stack>
      {truncatedLog ? (
        <Stack spacing={1}>
          <LogBlock label="Beginning of captured log" content={truncatedLog.beginning} />
          <TruncationDivider />
          <LogBlock label="End of captured log" content={truncatedLog.ending} />
        </Stack>
      ) : (
        <LogBlock
          label={log.truncated ? 'Available captured log' : 'Captured log'}
          content={content}
        />
      )}
    </Stack>
  );
}
