import { CircularProgress, Stack, Typography } from '@mui/material';

export interface LoadingStateProps {
  label: string;
  compact?: boolean;
}

export default function LoadingState({ label, compact = false }: LoadingStateProps) {
  return (
    <Stack
      role="status"
      aria-label={label}
      aria-live="polite"
      aria-busy="true"
      direction="row"
      spacing={1.5}
      alignItems="center"
      justifyContent="center"
      sx={{ minHeight: compact ? 72 : 144, color: 'text.secondary', p: 2 }}
    >
      <CircularProgress size={compact ? 24 : 32} aria-hidden="true" />
      <Typography color="inherit">{label}</Typography>
    </Stack>
  );
}
