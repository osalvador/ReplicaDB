import { Chip, type ChipProps } from '@mui/material';
import type { JobRunStatus } from '../api/runsApi';

export type StatusChipColor = NonNullable<ChipProps['color']>;

export const statusChipColors: Record<JobRunStatus, StatusChipColor> = {
  PENDING: 'info',
  RUNNING: 'primary',
  SUCCEEDED: 'success',
  FAILED: 'error',
  CANCEL_REQUESTED: 'warning',
  CANCELLED: 'default',
  RETRY_SCHEDULED: 'secondary'
};

export interface StatusChipProps extends Omit<ChipProps, 'color' | 'label'> {
  status?: JobRunStatus | null;
}

export default function StatusChip({ status, ...props }: StatusChipProps) {
  const label = status ?? 'UNKNOWN';
  const color = status ? statusChipColors[status] : 'default';

  return (
    <Chip
      {...props}
      role="status"
      aria-label={props['aria-label'] ?? `Run status: ${label}`}
      data-status-color={color}
      label={label}
      color={color}
      size={props.size ?? 'small'}
    />
  );
}
