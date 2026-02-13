# Safeguard Staging Table Drop

## Why

The `dropStagingTable()` method in `SqlManager` currently lacks safeguards to prevent dropping user-defined staging tables. While the `cleanUp()` method checks if a staging table is user-defined before calling drop, the drop method itself has no defensive validation. This creates a safety risk: if `dropStagingTable()` is ever called directly or through a different code path, it could destroy user data by dropping tables users explicitly configured.

Defensive programming principles dictate that methods should validate their own preconditions rather than relying solely on callers to enforce constraints.

## What Changes

Add a safety check inside `dropStagingTable()` to prevent dropping user-defined staging tables, making the method inherently safe regardless of how it's invoked.

## Capabilities

### Modified Capabilities
- `staging-table-cleanup`: Staging table drop operation now includes built-in safeguard to prevent dropping user-defined tables

## Impact

- [src/main/java/org/replicadb/manager/SqlManager.java](src/main/java/org/replicadb/manager/SqlManager.java): Add conditional check in `dropStagingTable()` method to skip drop when staging table is user-defined
- Removes TODO comment documenting the missing safeguard
- No behavior change for normal workflows—`cleanUp()` already performs this check, so this adds defense-in-depth
