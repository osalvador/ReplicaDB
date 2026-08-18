import { useQuery, useQueryClient } from '@tanstack/react-query';
import { createContext, type PropsWithChildren, useMemo } from 'react';
import { ApiError } from '../api/client';
import * as authApi from '../api/authApi';
import type { UserIdentityResponse } from '../api/authApi';

export type AuthStatus = 'loading' | 'authenticated' | 'anonymous';

export interface AuthContextValue {
  status: AuthStatus;
  user?: UserIdentityResponse;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

export const AuthContext = createContext<AuthContextValue | undefined>(undefined);

function isAnonymousError(error: unknown): boolean {
  return error instanceof ApiError && (error.status === 401 || error.status === 403);
}

export function AuthProvider({ children }: PropsWithChildren) {
  const queryClient = useQueryClient();
  const sessionQuery = useQuery({
    queryKey: ['auth', 'me'],
    queryFn: authApi.getMe,
    retry: (failureCount, error) => !isAnonymousError(error) && failureCount < 1
  });

  const login = async (username: string, password: string) => {
    const identity = await authApi.login(username, password);
    queryClient.setQueryData(['auth', 'me'], identity);
  };

  const logout = async () => {
    await authApi.logout();
    queryClient.setQueryData(['auth', 'me'], null);
  };

  const value = useMemo<AuthContextValue>(() => ({
    status: sessionQuery.isPending
      ? 'loading'
      : sessionQuery.data
        ? 'authenticated'
        : 'anonymous',
    user: sessionQuery.data ?? undefined,
    login,
    logout
  }), [sessionQuery.data, sessionQuery.isPending]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
