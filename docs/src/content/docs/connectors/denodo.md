---
title: Denodo connector
description: Denodo source-only behavior and JDBC query guidance.
---

# Denodo

Use `jdbc:vdb:`. Denodo is source-only: it can provide rows for complete,
complete-atomic, and incremental flows but cannot receive sink writes or
staging operations. Treat query pushdown, virtual view permissions, and type
conversion as Denodo responsibilities and validate the source query before
parallel execution.

Use `source.table` for a virtual view or `source.query` when Denodo must own a
join, projection, or filter. Confirm that the execution plan is pushed down as
intended and that the service account can read every underlying source. The
selected sink connector, not Denodo, determines staging and merge guarantees.
