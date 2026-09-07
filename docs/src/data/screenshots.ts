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
    key: 'job-new', filename: 'job-new.png', viewport: 'desktop', state: 'new job form',
    alt: 'ReplicaDB new job form', caption: 'A new job binds datasources and defines the replication contract.', guide: 'server/jobs'
  },
  {
    key: 'job-detail', filename: 'job-detail.png', viewport: 'desktop', state: 'complete-mode warning and run history',
    alt: 'ReplicaDB job detail with complete-mode warning', caption: 'Job detail keeps the destructive complete-mode warning beside the actions it affects.', guide: 'server/jobs'
  },
  {
    key: 'schedule', filename: 'schedule.png', viewport: 'desktop', state: 'schedule configuration dialog',
    alt: 'ReplicaDB schedule configuration dialog', caption: 'The schedule editor builds a Quartz CRON expression with an explicit time zone and enabled state.', guide: 'server/schedules'
  },
  {
    key: 'job-edit', filename: 'job-edit.png', viewport: 'desktop', state: 'edit job form',
    alt: 'ReplicaDB edit job form', caption: 'The edit form preserves datasource references and retry policy controls.', guide: 'server/jobs'
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
    key: 'datasource-new', filename: 'datasource-new.png', viewport: 'desktop', state: 'new datasource form',
    alt: 'ReplicaDB new datasource form', caption: 'Datasource creation separates technical settings from protected security values.', guide: 'server/datasources'
  },
  {
    key: 'datasource-detail', filename: 'datasource-detail.png', viewport: 'desktop', state: 'redacted datasource detail',
    alt: 'ReplicaDB datasource detail with redacted connection', caption: 'Datasource detail never rehydrates stored credentials.', guide: 'server/datasources'
  },
  {
    key: 'datasource-edit', filename: 'datasource-edit.png', viewport: 'desktop', state: 'secret-preserving edit form',
    alt: 'ReplicaDB edit datasource form', caption: 'Blank security fields preserve encrypted values until explicitly cleared.', guide: 'server/datasources'
  },
  {
    key: 'datasource-permissions', filename: 'datasource-permissions.png', viewport: 'desktop', state: 'grant dialog',
    alt: 'ReplicaDB datasource permissions', caption: 'Admins grant VIEW, USE, and EDIT resource permissions.', guide: 'server/permissions'
  },
  {
    key: 'job-permissions', filename: 'job-permissions.png', viewport: 'desktop', state: 'job permission matrix',
    alt: 'ReplicaDB job permissions', caption: 'Admins grant VIEW, EDIT, EXECUTE, and CANCEL permissions.', guide: 'server/permissions'
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
    key: 'profile', filename: 'profile.png', viewport: 'desktop', state: 'authenticated profile',
    alt: 'ReplicaDB profile page', caption: 'The profile page shows the current identity and role without offering secret self-service.', guide: 'server/sign-in-and-profile'
  },
  {
    key: 'unauthorized', filename: 'unauthorized.png', viewport: 'desktop', state: 'unauthorized route state',
    alt: 'ReplicaDB unauthorized page', caption: 'Route visibility does not replace backend authorization.', guide: 'server/errors-and-empty-states'
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
    key: 'jobs-mobile', filename: 'jobs-mobile.png', viewport: 'mobile', state: 'mobile job catalog',
    alt: 'ReplicaDB mobile jobs catalog', caption: 'The jobs catalog remains usable on a narrow viewport.', guide: 'server/jobs'
  },
  {
    key: 'datasources-mobile', filename: 'datasources-mobile.png', viewport: 'mobile', state: 'mobile datasource catalog',
    alt: 'ReplicaDB mobile datasource catalog', caption: 'Datasource rows remain contained on a narrow viewport.', guide: 'server/datasources'
  },
  {
    key: 'run-detail-mobile', filename: 'run-detail-mobile.png', viewport: 'mobile', state: 'mobile run diagnostics',
    alt: 'ReplicaDB mobile run detail', caption: 'Run diagnostics remain inspectable on mobile.', guide: 'server/runs-and-diagnostics'
  }
];
