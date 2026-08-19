import { Outlet } from 'react-router-dom';
import { useAuth } from '../auth/useAuth';
import NotAuthorizedPage from '../pages/NotAuthorizedPage';

export type GlobalRole = 'ADMIN' | 'OPERATOR' | 'VIEWER';

export default function RequireRole({ role }: { role: GlobalRole }) {
  const { user } = useAuth();

  return user?.role === role ? <Outlet /> : <NotAuthorizedPage />;
}
