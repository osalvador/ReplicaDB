---
type: Decision
description: ReplicaDB keeps a Spring-free CLI artifact beside a managed server artifact in one codebase.
sources:
  - id: decision
    resource: ARCHITECTURE_DECISIONS.md
  - id: root-pom
    resource: pom.xml
  - id: server-pom
    resource: replicadb-server/pom.xml
  - id: guard
    resource: src/test/java/org/replicadb/NoSpringBootOnClasspathTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Driving forces: preserve the existing CLI footprint, startup, options-file contract, exit codes, and ability to run without a metadata database while adding managed scheduling and monitoring.

Decision: keep one codebase with a root CLI artifact and sibling `replicadb-server`. The server translates stored jobs to `ToolOptions` and reuses the core manager implementation. Spring Boot is not introduced into the CLI classpath.

Trade-off: there are two build/version surfaces and the server must install or resolve the core artifact before compilation. The boundary avoids duplicating replication behavior and keeps CLI compatibility testable.
