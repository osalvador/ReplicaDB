---
type: Learning
description: Inspect profile-specific test resources whenever a Spring context disagrees with production configuration.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

The API test profile supplied its own `application-api.yml` and silently omitted the production Quartz JDBC cluster properties. The new clustered-required guard therefore rejected a valid production configuration during tests.

Synchronizing the test profile with the production settings restored the intended context assertion. Test resources that override a profile must be treated as configuration surfaces and checked whenever a context test observes an unexpected default.
