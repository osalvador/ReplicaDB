import type { components } from './schema';
import { apiClient } from './client';

export type UserResponse = components['schemas']['UserResponse'];
export type UserRequest = components['schemas']['UserRequest'];
export type RoleUpdate = components['schemas']['RoleUpdate'];
export type PasswordUpdate = components['schemas']['PasswordUpdate'];
export type UserPage = Omit<components['schemas']['PageResponseUserResponse'], 'content'> & {
  content?: UserResponse[];
};

export async function listUsers(page = 0, size = 50): Promise<UserPage> {
  const response = await apiClient.get<UserPage>('/users', {
    params: { page, size }
  });
  return response.data;
}

export async function createUser(request: UserRequest): Promise<UserResponse> {
  const response = await apiClient.post<UserResponse>('/users', request);
  return response.data;
}

export async function updateUserRole(id: string, request: RoleUpdate): Promise<UserResponse> {
  const response = await apiClient.put<UserResponse>(`/users/${id}`, request);
  return response.data;
}

export async function updateUserPassword(id: string, request: PasswordUpdate): Promise<UserResponse> {
  const response = await apiClient.put<UserResponse>(`/users/${id}/password`, request);
  return response.data;
}
