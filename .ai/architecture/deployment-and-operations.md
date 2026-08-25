---
type: Deployment
description: Maven, frontend-maven-plugin, launchers, containers, environment configuration, and Testcontainers support the CLI and managed server.
sources:
  - id: root-pom
    resource: pom.xml
  - id: server-pom
    resource: replicadb-server/pom.xml
  - id: application
    resource: replicadb-server/src/main/resources/application.yml
  - id: api-config
    resource: replicadb-server/src/main/resources/application-api.yml
  - id: docker
    resource: Dockerfile
  - id: container
    resource: Containerfile
  - id: dependabot
    resource: .github/dependabot.yml
  - id: ci
    resource: .github/workflows
  - id: deployment-guide
    resource: DEPLOYMENT.md
  - id: server-image
    resource: replicadb-server/Dockerfile
  - id: compose
    resource: docker-compose.server.yml
  - id: docs-check
    resource: scripts/check-phase3-docs.sh
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

The root build targets Java 17 and packages the standalone CLI with vendor drivers and Log4j2, including the current MariaDB JDBC driver `3.5.10`. The server module pins the core artifact, uses Spring Boot 3.3.5, PostgreSQL/Flyway, Quartz, Spring Security/Session, OpenAPI, and `frontend-maven-plugin` to run pinned Node/npm build steps and copy SPA assets into server resources.

Managed runtime configuration is environment-driven for datasource and bootstrap concerns. The API profile enables Flyway, JDBC sessions, and a PostgreSQL-backed clustered Quartz store; product schedules remain reconciled from PostgreSQL. The worker profile disables its product listener with `server.port=-1` and exposes only an internal Actuator management port. PostgreSQL `now()` owns claim eligibility, lease timestamps, and expiry backoff. The managed server image runs as a non-root user, and Compose provides two APIs plus one or more workers on separated public/control networks. Testcontainers cover multiple database families, with architecture, resource, reuse, and vendor-image health treated as separate infrastructure concerns.

CI runs separate database integration, non-integration, server, frontend E2E, multi-node resilience/load, and packaging jobs with Docker/Testcontainers configuration; frontend E2E also regenerates and diffs the OpenAPI TypeScript schema. Server gates cover 16 Flyway migrations, image smoke, documentation checks, and the worker profile. Dependabot covers Maven, Bundler, and GitHub Actions, with hardened workflow-run checks before auto-merge and a manual rebase workflow.

Phase 3.3 distributed workers, notification dispatch, shared login throttling, health/metrics, persistent Quartz clustering, server packaging, and process-level resilience/load validation are implemented and validated. Phase 3.4 hybrid worker load distribution remains an approved follow-up.

Reference implementations: `pom.xml`, `replicadb-server/pom.xml`, `replicadb-server/Dockerfile`, `docker-compose.server.yml`, `DEPLOYMENT.md`, and `replicadb-server/src/main/resources/application-api.yml`.
