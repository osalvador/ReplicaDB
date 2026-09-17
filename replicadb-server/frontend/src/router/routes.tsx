import { createBrowserRouter, type RouteObject } from 'react-router-dom';
import AppLayout from '../layout/AppLayout';
import DashboardPage from '../pages/DashboardPage';
import AuditPage from '../pages/AuditPage';
import JobsPage from '../pages/JobsPage';
import DatasourceFormPage from '../pages/DatasourceFormPage';
import DatasourceDetailPage from '../pages/DatasourceDetailPage';
import DatasourcePermissionsPage from '../pages/DatasourcePermissionsPage';
import DatasourcesPage from '../pages/DatasourcesPage';
import JobDetailPage from '../pages/JobDetailPage';
import JobFormPage from '../pages/JobFormPage';
import JobPermissionsPage from '../pages/JobPermissionsPage';
import LoginPage from '../pages/LoginPage';
import UsersPage from '../pages/UsersPage';
import RunDetailPage from '../pages/RunDetailPage';
import ProfilePage from '../pages/ProfilePage';
import ProtectedRoute from './ProtectedRoute';
import RequireRole from './RequireRole';

export const routeObjects: RouteObject[] = [
  {
    path: '/login',
    element: <LoginPage />
  },
  {
    path: '/',
    element: <AppLayout />,
    children: [
      {
        element: <ProtectedRoute />,
        children: [
          { index: true, element: <DashboardPage /> },
          { path: 'profile', element: <ProfilePage /> },
                    { path: 'jobs', element: <JobsPage /> },
          { path: 'datasources', element: <DatasourcesPage /> },
          { path: 'datasources/:id', element: <DatasourceDetailPage /> },
          { path: 'datasources/:id/edit', element: <DatasourceFormPage /> },
          { path: 'jobs/new', element: <JobFormPage /> },
          { path: 'jobs/:id/edit', element: <JobFormPage /> },
          { path: 'jobs/:id', element: <JobDetailPage /> },
          { path: 'runs/:id', element: <RunDetailPage /> },
          {
            element: <RequireRole role="ADMIN" />,
            children: [
              { path: 'audit', element: <AuditPage /> },
              { path: 'datasources/new', element: <DatasourceFormPage /> },
              { path: 'datasources/:id/permissions', element: <DatasourcePermissionsPage /> },
              { path: 'users', element: <UsersPage /> },
              { path: 'jobs/:id/permissions', element: <JobPermissionsPage /> }
            ]
          }
        ]
      }
    ]
  }
];

export const router = createBrowserRouter(routeObjects);
