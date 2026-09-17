---
title: SQLite connector
description: SQLite file-based JDBC behavior and mode limitations.
---


Use the `jdbc:sqlite` scheme for a file or in-memory database. SQLite can be a
source or sink for complete and incremental replication. Its sink does not
support complete-atomic mode, and file locking can make concurrency the
bottleneck before the `jobs` setting is reached.

Use a stable path owned by the process and avoid placing a live database file
inside an archive or shared workspace. Incremental merges depend on the sink
primary key and do not propagate deletes.

SQLite accepts table and free-form query reads, but its single-file locking
model usually limits useful write concurrency. Keep the database on a local,
durable filesystem, back it up with SQLite-aware tooling, and inspect the sink
after an interrupted complete or incremental run.
