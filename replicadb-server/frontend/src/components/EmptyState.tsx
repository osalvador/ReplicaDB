import { Stack, Typography } from '@mui/material';
import type { ReactNode } from 'react';

export interface EmptyStateProps {
  title: ReactNode;
  description?: ReactNode;
  action?: ReactNode;
}

export default function EmptyState({ title, description, action }: EmptyStateProps) {
  const accessibleLabel = typeof title === 'string' ? title : 'Empty state';

  return (
    <Stack
      role="status"
      aria-label={accessibleLabel}
      spacing={0.5}
      alignItems="center"
      justifyContent="center"
      sx={{ minHeight: 96, p: 2, textAlign: 'center' }}
    >
      <Typography color="text.secondary" fontWeight={600}>
        {title}
      </Typography>
      {description !== undefined && (
        <Typography color="text.secondary" variant="body2">
          {description}
        </Typography>
      )}
      {action !== undefined && <Stack sx={{ mt: 1 }}>{action}</Stack>}
    </Stack>
  );
}
