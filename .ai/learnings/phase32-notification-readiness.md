# Notification Listener Readiness

type: Learning
sources: [{ id: plan, resource: .ai/archive/phase-3-2-worker-runtime-and-postgresql-dispatch.plan.md }]
generated: { by: itx-code, at: 2026-08-24 }

A PostgreSQL notification published immediately after starting an asynchronous listener can be lost before `LISTEN` completes. Synchronize integration publication with a successful subscription/reconnect callback, and test missed delivery separately through durable startup or periodic polling rather than assuming notification delivery.
