import type { JobRunStatus } from '../api/runsApi';

export function isTerminalRunStatus(status: JobRunStatus | null | undefined): boolean {
  return status === 'SUCCEEDED' || status === 'CANCELLED' || status === 'RETRY_SCHEDULED';
}

export function getRunRefetchInterval(status: JobRunStatus | null | undefined): number | false {
  return isTerminalRunStatus(status) ? false : 5000;
}
