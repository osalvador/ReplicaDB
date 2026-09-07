---
type: Learning
sources:
  - id: plan
    resource: .ai/archive/release-1.0.0-controlada.plan.md
generated:
  by: itx-code
  at: 2026-09-06
---

Windows embedded PostgreSQL release smoke must use a user-owned server home;
runner temporary paths can reject `initdb` permission changes. Batch launchers
also need bounded readiness probes, direct PID ownership, and diagnostic logs.
The final release uses the profile home, bounded `curl.exe` health checks, and a
PowerShell direct PID write.
