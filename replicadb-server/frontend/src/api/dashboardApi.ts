import type { components } from './schema';
import { apiClient } from './client';

export type DashboardSummaryResponse = components['schemas']['DashboardSummaryResponse'];
export type DashboardWindow = { from: string; to: string };

export async function getDashboardSummary(window: DashboardWindow): Promise<DashboardSummaryResponse> {
  const response = await apiClient.get<DashboardSummaryResponse>('/dashboard/summary', {
    params: window
  });
  return response.data;
}
