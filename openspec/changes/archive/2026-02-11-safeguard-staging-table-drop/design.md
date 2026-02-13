# Safeguard Staging Table Drop - Design

## Context

ReplicaDB uses staging tables in two modes:
- **Incremental mode**: Creates staging table, loads new data, merges with target
- **Complete-atomic mode**: Creates staging table, loads all data, atomically swaps

Staging tables can be:
1. **Auto-generated**: ReplicaDB creates with generated name (e.g., `sink_table_stg`)
2. **User-defined**: User specifies via `--sink-staging-table` option

Currently, `cleanUp()` method checks if staging table is user-defined before calling `dropStagingTable()`, but the drop method itself has no safeguard. This violates defensive programming—methods should be self-protecting.

## Goals / Non-Goals

**Goals:**
- Make `dropStagingTable()` inherently safe by adding internal validation
- Preserve user-defined tables regardless of how method is called
- Remove TODO comment once safeguard is implemented
- Maintain existing behavior (no functional changes, just added safety)

**Non-Goals:**
- Change how `cleanUp()` works (it already does the right thing)
- Modify staging table creation logic
- Add new configuration options
- Change logging beyond adding skip message

## Decisions

### Decision 1: Add validation inside dropStagingTable()

**Approach**: Add conditional check at the start of `dropStagingTable()` method:
```java
if (options.getSinkStagingTable() != null && !options.getSinkStagingTable().isEmpty()) {
    LOG.info("Skipping staging table drop because it is user-defined: {}", 
             getQualifiedStagingTableName());
    return;
}
```

**Rationale:**
- Early return pattern is clear and prevents nested conditionals
- Reuses same check that `cleanUp()` already uses (consistent logic)
- Log message provides visibility when table is preserved
- No changes to method signature or exception handling

### Decision 2: Remove TODO comment

The TODO at line 489 documents this missing safeguard. Once implemented, remove it.

### Decision 3: Keep redundant check in cleanUp()

The check in `cleanUp()` that calls `dropStagingTable()` could be removed since the method now self-validates. However, keeping it provides:
- Defense-in-depth (double protection)
- Avoids unnecessary method calls
- Maintains explicit intent in cleanup logic

**Decision**: Keep the existing check in `cleanUp()`. No changes needed there.

## Implementation Notes

- Change is isolated to `SqlManager.dropStagingTable()` method
- No test changes needed—existing tests for incremental/atomic modes already verify staging tables are dropped appropriately
- No behavioral change for existing workflows
