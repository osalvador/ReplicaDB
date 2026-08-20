---
type: Learning
description: Major dependency upgrades require resolved API, bytecode, and dependency-family validation before merge.
sources:
  - id: mariadb-code
    resource: src/main/java/org/replicadb/manager/MySQLManager.java
  - id: dependency-baseline
    resource: pom.xml
  - id: orc-code
    resource: src/main/java/org/replicadb/manager/file/OrcFileManager.java
  - id: dependabot-policy
    resource: .github/dependabot.yml
generated: { by: itx-init/2.1, at: "2026-08-20T12:55:57Z" }
status: stable
---

The MariaDB Connector/J major upgrade removed the proprietary `MariaDbStatement` class used by the MySQL manager. The compatible public `org.mariadb.jdbc.Statement` API had to be confirmed from the resolved jar and adopted before the upgrade could compile. Hive Storage API 4.2.0 instead introduced Java 21 bytecode into a Java 17 build and was also coupled to the ORC/Hadoop dependency family.

For every major dependency proposal, inspect the resolved jar API and bytecode, run a clean compile with the supported language/runtime baseline, and check related dependency families together. Keep incompatible major upgrades out of Dependabot's automatic flow until the migration is coordinated.