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
generated: { by: itx-init/2.1, at: "2026-08-20T11:06:29Z" }
status: stable
---

The root build targets Java 17 and packages the standalone CLI with vendor drivers and Log4j2, including the current MariaDB JDBC driver `3.5.10`. The server module pins the core artifact, uses Spring Boot 3.3.5, PostgreSQL/Flyway, Quartz, Spring Security/Session, OpenAPI, and `frontend-maven-plugin` to run pinned Node/npm build steps and copy SPA assets into server resources.

Managed runtime configuration is environment-driven for datasource and bootstrap concerns. The API profile enables Flyway and JDBC sessions; the default Quartz runtime store is memory-backed and product schedules are reconciled from PostgreSQL. Docker and Podman-compatible files package the CLI/runtime surfaces. Testcontainers cover multiple database families, with architecture, resource, reuse, and vendor-image health treated as separate infrastructure concerns.

CI runs separate database integration, non-integration, server, frontend E2E, and packaging jobs with Docker/Testcontainers configuration; frontend E2E also regenerates and diffs the OpenAPI TypeScript schema. Dependabot covers Maven, Bundler, and GitHub Actions, with hardened workflow-run checks before auto-merge and a manual rebase workflow.

Phase 3 distributed workers, notification dispatch, and a production-ready persistent Quartz job store are future or operational decisions rather than current observed deployment behavior.

Reference implementations: `pom.xml`, `replicadb-server/pom.xml`, `Dockerfile`, and `replicadb-server/src/main/resources/application-api.yml`.
