import type { components } from './schema';
import { apiClient } from './client';

export type UserIdentityResponse = components['schemas']['UserIdentityResponse'];
export type LoginRequest = components['schemas']['LoginRequest'];

export async function getMe(): Promise<UserIdentityResponse> {
  const response = await apiClient.get<UserIdentityResponse>('/auth/me');
  return response.data;
}

export async function getCsrf(): Promise<void> {
  await apiClient.get('/auth/csrf');
}

export async function login(username: string, password: string): Promise<UserIdentityResponse> {
  await getCsrf();
  const request: LoginRequest = { username, password };
  const response = await apiClient.post<UserIdentityResponse>('/auth/login', request);
  return response.data;
}

export async function logout(): Promise<void> {
  await apiClient.post('/auth/logout');
}
