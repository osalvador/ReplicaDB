---
title: MySQL and MariaDB connectors
description: MySQL and MariaDB source and sink behavior and driver guidance.
---

# MySQL and MariaDB

Use `jdbc:mysql:` for MySQL or `jdbc:mariadb:` for MariaDB. Both managers can
be source or sink and support complete, complete-atomic, and incremental modes.
The maintained driver is MariaDB JDBC for both paths.

Bulk loading and driver options can change throughput and security behavior.
Validate `LOCAL INFILE` policy with the database administrator before using a
bulk path. Staging tables are used for complete-atomic and incremental sink
flows; keep their schema and permissions explicit.