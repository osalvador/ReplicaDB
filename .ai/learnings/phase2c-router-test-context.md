---
type: Learning
description: Mount parameterized React Router pages through a matching route in tests rather than only supplying memory history.
sources:
  - id: plan
    resource: .ai/archive/phase-2c-frontend-administration.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`JobPermissionsPage` uses `useParams`; a bare `MemoryRouter` left its ID undefined and disabled its queries. Test parameterized screens with matching `Routes` and `Route` entries so the harness supplies the same route context as production.
