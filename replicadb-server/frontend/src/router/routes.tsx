import { createBrowserRouter, type RouteObject } from 'react-router-dom';
import AppLayout from '../layout/AppLayout';
import DashboardPage from '../pages/DashboardPage';
import JobDetailPage from '../pages/JobDetailPage';
import JobFormPage from '../pages/JobFormPage';
import LoginPage from '../pages/LoginPage';
import RunDetailPage from '../pages/RunDetailPage';
import ProtectedRoute from './ProtectedRoute';

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
          { path: 'jobs/new', element: <JobFormPage /> },
          { path: 'jobs/:id/edit', element: <JobFormPage /> },
          { path: 'jobs/:id', element: <JobDetailPage /> },
          { path: 'runs/:id', element: <RunDetailPage /> }
        ]
      }
    ]
  }
];

export const router = createBrowserRouter(routeObjects);
