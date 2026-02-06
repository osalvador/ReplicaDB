---
applyTo: '**'
---

# Git Commit Conventions

## Commit Message Format

ReplicaDB follows **Conventional Commits** with semantic prefixes for changelog generation and semantic versioning.

### Standard Format
```
<type>: <subject>

[optional body]

[optional footer]
```

### Commit Types

- **feat**: New feature or capability (increments MINOR version)
- **fix**: Bug fix (increments PATCH version)
- **docs**: Documentation changes only
- **test**: Adding or updating tests
- **refactor**: Code restructuring without behavior change
- **perf**: Performance improvements
- **chore**: Build process, dependencies, tooling
- **revert**: Reverts a previous commit

### Examples from Project History

**Feature additions**:
```
feat: Add robust NUMERIC binary encoding with text fallback
feat: Improve PostgreSQL binary COPY with MySQL/MongoDB compatibility
```

**Bug fixes**:
```
fix: Correct DB2 test schema - use BIGINT instead of NUMERIC(19)
fix: Fix DB2 JDBC compatibility by following JDBC spec for wasNull() calls
fix: Add null checks for DATE/TIME/TIMESTAMP in binary encoding
```

**Documentation**:
```
docs: Add comprehensive plan for schema-flexible binary COPY
```

**Reverts**:
```
revert: Re-enable binary COPY for all database sources
```

**Chores**:
```
Exclude specific test from Maven build in CI workflow
Update CT_Push.yml
```

## Subject Line Rules

1. **Imperative mood**: "Add feature" not "Added feature" or "Adds feature"
2. **No period**: Subject lines don't end with punctuation
3. **50 characters max**: Keep subjects concise
4. **Lowercase after colon**: `feat: add support` not `feat: Add support`

## Body Guidelines

- Wrap at 72 characters for readability in `git log`
- Explain **what** and **why**, not **how** (code shows how)
- Reference issue numbers when applicable: `Fixes #123` or `Relates to #456`
- Break into paragraphs for complex changes

## Breaking Changes

Mark breaking changes in commit footer:
```
feat: change CLI argument format for parallel jobs

BREAKING CHANGE: --jobs parameter now requires explicit value.
Previous: --jobs (defaulted to 4)
Current: --jobs=4 (required value)
```

## Scope Usage (Optional)

Add scope for module-specific changes:
```
fix(postgres): correct binary COPY encoding for timestamp types
feat(oracle): add SDO_GEOMETRY spatial type support
test(db2): add integration tests for LOB types
```

**Common scopes**: postgres, oracle, sqlserver, mysql, mongodb, s3, kafka, cli, manager, test

## Multi-Line Commit Example

```
feat: add binary COPY support for PostgreSQL

Implement PostgreSQL binary COPY protocol to achieve 10x faster
inserts compared to standard JDBC batch inserts. This optimization
is crucial for large-scale data migrations.

Technical changes:
- Add PGConnection COPY API integration
- Implement ResultSetInputStream for streaming conversion
- Add binary encoders for all PostgreSQL data types

Performance benchmarks show:
- 1M rows: 45 seconds (binary COPY) vs 7 minutes (JDBC batch)
- 10M rows: 8 minutes (binary COPY) vs 1.2 hours (JDBC batch)

Relates to #234
```

## Commit Frequency

- **Atomic commits**: One logical change per commit
- **Working state**: Each commit should compile and pass tests
- **Feature branches**: Squash before merge if commits are WIP/experimental
- **Main branch**: All commits maintain backward compatibility

## Pull Request Conventions

- PR title follows commit format: `feat: add DB2 support`
- Link related issues in PR description
- Include test results for database-specific changes
- Update README.md version references when releasing

## Version Tagging

- Tags follow semantic versioning: `v0.17.0`, `v0.16.1`
- Tag messages include changelog highlights
- Release commits: `Release v0.17.0`
