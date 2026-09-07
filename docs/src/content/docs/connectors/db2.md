---
title: IBM Db2 connector
description: Db2 LUW and IBM i source/sink modes and JDBC caveats.
---

# IBM Db2

Use `jdbc:db2:` for Db2 LUW and `jdbc:as400:` for IBM i. Both variants support
source and sink roles, all three replication modes, staging, partitioned reads,
and bandwidth throttling.

Driver versions, naming conventions, permissions, and type mappings differ
between Db2 environments. Validate ROW_NUMBER-based partitioning, LOB values,
and generated staging-table permissions with a representative schema.