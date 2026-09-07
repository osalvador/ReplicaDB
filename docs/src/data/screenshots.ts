export type ScreenshotDefinition = {
  key: string;
  filename: string;
  viewport: 'desktop' | 'mobile';
  state: string;
  alt: string;
  caption: string;
  guide: string;
};

export const screenshotDefinitions: ScreenshotDefinition[] = [
  {
    key: 'login', filename: 'login.png', viewport: 'desktop', state: 'empty sign-in form',
    alt: 'ReplicaDB sign-in form', caption: 'The authenticated entry point for the managed control plane.', guide: 'server/sign-in-and-profile'
  },
  {
    key: 'dashboard', filename: 'dashboard.png', viewport: 'desktop', state: 'seeded operational summary',
    alt: 'ReplicaDB dashboard with run metrics', caption: 'The dashboard summarizes jobs, active runs, outcomes, rows, duration, and queue latency.', guide: 'server/dashboard'
  },
  {
    key: 'jobs', filename: 'jobs.png', viewport: 'desktop', state: 'seeded job catalog',
    alt: 'ReplicaDB jobs catalog', caption: 'The jobs catalog lists definitions available to the signed-in operator.', guide: 'server/jobs'
  },
  {
    key: 'run-detail', filename: 'run-detail.png', viewport: 'desktop', state: 'terminal run diagnostics',
    alt: 'ReplicaDB run detail and bounded diagnostics', caption: 'Run detail shows status, metrics, and bounded operational logs.', guide: 'server/runs-and-diagnostics'
  },
  {
    key: 'datasources', filename: 'datasources.png', viewport: 'desktop', state: 'datasource catalog',
    alt: 'ReplicaDB datasource catalog', caption: 'Datasource profiles expose safe metadata and permission flags.', guide: 'server/datasources'
  },
  {
    key: 'users', filename: 'users.png', viewport: 'desktop', state: 'admin user management',
    alt: 'ReplicaDB user management', caption: 'Admins manage roles, enabled state, and password resets.', guide: 'server/users'
  },
  {
    key: 'audit', filename: 'audit.png', viewport: 'desktop', state: 'filtered audit history',
    alt: 'ReplicaDB audit history', caption: 'Admins filter and inspect durable audit events.', guide: 'server/audit'
  },
  {
    key: 'login-mobile', filename: 'login-mobile.png', viewport: 'mobile', state: 'mobile sign-in layout',
    alt: 'ReplicaDB mobile sign-in form', caption: 'The sign-in flow remains readable on a narrow viewport.', guide: 'server/sign-in-and-profile'
  },
  {
    key: 'dashboard-mobile', filename: 'dashboard-mobile.png', viewport: 'mobile', state: 'mobile dashboard layout',
    alt: 'ReplicaDB mobile dashboard', caption: 'Dashboard metrics stack without horizontal overflow on mobile.', guide: 'server/dashboard'
  },
  {
    key: 'run-detail-mobile', filename: 'run-detail-mobile.png', viewport: 'mobile', state: 'mobile run diagnostics',
    alt: 'ReplicaDB mobile run detail', caption: 'Run diagnostics remain inspectable on mobile.', guide: 'server/runs-and-diagnostics'
  }
];