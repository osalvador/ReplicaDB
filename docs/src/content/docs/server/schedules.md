---
title: Schedules
description: Configure and remove recurring job schedules.
---

# Schedules

The job detail page owns the schedule card. A job without a schedule shows
`No recurring schedule configured` and the `Create schedule` action. An
existing schedule shows its CRON expression, time zone, enabled state, and
next fire time, with `Edit` and a destructive delete action.

The editor accepts a frequency builder or a `CRON expression`, a `Time zone`,
and `Enabled`. Save with `Save` or leave with `Cancel`. Schedule validation
errors remain in the editor; API failures use an operational error notice.

Scheduling triggers durable runs; it does not bypass job permissions or
datasource bindings. A disabled binding blocks future scheduled work while an
active run continues under its own lifecycle.