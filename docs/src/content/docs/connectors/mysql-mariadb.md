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

The optimized sink path uses `LOAD DATA LOCAL INFILE` through an in-memory
stream. The server and client policy must permit local loading; enabling it
changes the driver's data-loading surface, so approve it explicitly rather
than weakening a global database policy. ReplicaDB escapes text and converts
binary values for this path, but representative BLOB, CLOB, date, and JSON
values still require a compatibility run.

The MariaDB JDBC driver handles both documented schemes. Use a scheme and
driver properties that match the actual server, and keep TLS/authentication
parameters under the corresponding `source.connect.parameter.*` or
`sink.connect.parameter.*` namespace.
