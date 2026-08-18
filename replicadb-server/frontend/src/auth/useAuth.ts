import { useContext } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { AuthContext } from './AuthContext';

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used inside AuthProvider');
  }

  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const logout = async () => {
    await context.logout();
    queryClient.clear();
    navigate('/login');
  };

  return { ...context, logout };
}
