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

Partitioned source reads use a `ROW_NUMBER` window over the selected table
shape. Verify the ordering and query plan on the target Db2 edition before
raising `jobs`; a syntactically valid query can still put excessive work on
the source.

Db2 LUW and IBM i use different JDBC schemes, drivers, catalog conventions,
and identifier rules. BLOB and CLOB transfer follows JDBC type handling, so
test null, empty, and large LOB values rather than assuming LUW and IBM i
behave identically. Complete-atomic and incremental sink modes also require
create/drop or lifecycle rights for their staging table.
