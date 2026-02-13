# Staging Table Cleanup

## ADDED Requirements

### Requirement: Prevent User-Defined Staging Table Deletion

The `dropStagingTable()` method MUST NOT drop staging tables that were explicitly defined by users through the `--sink-staging-table` option. Only automatically-generated staging tables SHALL be dropped.

#### Scenario: Auto-generated staging table is dropped

- **WHEN** `dropStagingTable()` is called
- **AND** the staging table was automatically created by ReplicaDB (not user-defined)
- **THEN** the staging table is dropped from the database
- **AND** the operation completes successfully

#### Scenario: User-defined staging table is preserved

- **WHEN** `dropStagingTable()` is called
- **AND** the staging table was explicitly defined by the user via `getSinkStagingTable()` returning a non-null, non-empty value
- **THEN** the staging table is NOT dropped
- **AND** a log message indicates the table was skipped because it is user-defined
- **AND** no SQL DROP statement is executed

#### Scenario: Method is safe regardless of caller

- **WHEN** `dropStagingTable()` is called from any code path (not just `cleanUp()`)
- **THEN** the method MUST validate whether the table is user-defined before attempting to drop
- **AND** user data is protected even if called incorrectly
