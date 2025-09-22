---
applyTo: '**'
---

# ReplicaDB: Git Commit Standards

## Commit Message Format

ReplicaDB follows **conventional commit standards** with specific patterns observed in the repository:

### Standard Format
```
<type>: <description>

[optional body]

[optional footer]
```

### Commit Types (from analysis of 50+ recent commits)

**feat**: New features or capabilities
```
feat: upgrade to jdk11
feat: refactor  
feat: MongDB implementation
feat: Transform Mongo document and arrays to json string
```

**fix**: Bug fixes and corrections
```
fix: handle blob data from mysql to postgres
fix: remove com.sun dependencies
fix: #130 retrieve the number of rows processed
fix: #128 return the exit code from ReplicaDB process
```

**build**: Build system and dependency changes
```
build(deps): bump org.postgresql:postgresql from 42.3.7 to 42.7.2
build(deps): bump sqlite-jdbc from 3.36.0.3 to 3.41.2.2
```

**test**: Test additions and modifications
```
test: Oracle 2 SqlServer
test: fix MySQL2PostgresIncremental
test: created test for MongoDB to Postgres
```

**chore**: Maintenance and tooling
```
chore: upgrade google guava
```

**refactor**: Code improvements without functional changes
```
refactor: deprecated sun CachedRowSetImpl to StreamingRowSetImpl
refactor: log SQLServer mapping errors
```

**doc**: Documentation updates
```
doc: update readme
```

**release**: Version releases
```
release v0.15.0
release: v0.14.0
```

## Branch Naming Conventions

### Pattern Analysis from Repository
Based on merge commit analysis, the repository uses:

**Feature branches**:
- `feature/podman-ubi9`
- `feature/[descriptive-name]`

**Dependency updates** (automated):
- Dependabot creates branches automatically
- Format: `dependabot/maven/[group]-[artifact]-[version]`

**Personal forks**:
- Contributors create descriptive branch names in their forks
- Merged via pull requests to `master`

### Recommended Branch Naming
```
feature/database-support-[database-name]     # New database implementations
fix/issue-[number]-[short-description]       # Bug fixes referencing issues
enhancement/performance-[area]               # Performance improvements
update/dependency-[library-name]             # Manual dependency updates
```

## Pull Request Requirements

### Based on Repository Pattern Analysis

**Title Format**: Clear, descriptive titles that explain the change
```
✅ Good: "Update to support BIT data type in MySQL/MariaDB tables"
✅ Good: "Replicar campos CLOB - Oracle to Oracle"
❌ Avoid: "Fix bug" or "Update code"
```

**PR Description Requirements**:
- **Problem description**: What issue does this solve?
- **Solution approach**: How was it implemented?
- **Testing**: What tests were added/modified?
- **Breaking changes**: Any configuration or API changes?

**Review Process**:
- All PRs require review from project maintainer (@osalvador)
- Automated dependency updates (Dependabot) are automatically merged
- Feature PRs require manual testing with multiple database combinations

## Merge Strategy

### Current Repository Patterns
- **Merge commits**: Repository uses merge commits (not squash)
- **Preserve history**: Individual commit history maintained in merges
- **Master branch**: All changes merge to `master` branch

Example merge commit pattern:
```
Merge pull request #180 from stalincalderon/master

Replicar campos CLOB - Oracle to Oracle
```

## Commit Message Guidelines

### Scope and Description Patterns

**Database-specific changes**:
```
feat: add MongoDB aggregation support
fix: handle CLOB fields in Oracle to Oracle replication
test: add cross-database BIT datatype tests
```

**Performance improvements**:
```
fix: total number of rows fetched in RowSetImpl
feat: refactor bandwidth throttling implementation
```

**Dependency and build changes**:
```
build(deps): bump jackson-databind from 2.12.6.1 to 2.12.7.1
chore: upgrade google guava
feat: upgrade to jdk11
```

**Issue references** (when applicable):
```
fix: #130 retrieve the number of rows processed
fix: #128 return the exit code from ReplicaDB process
```

### Message Body Guidelines

**For complex features**:
```
feat: MongDB implementation

- Add MongoDBManager extending ConnManager
- Implement aggregation-based data reading  
- Support MongoDB to SQL database replication
- Handle BSON to SQL type conversions

first commit, still unstable
```

**For dependency updates**:
```
build(deps): bump org.postgresql:postgresql from 42.3.7 to 42.7.2

Bumps [org.postgresql:postgresql](https://github.com/pgjdbc/pgjdbc) from 42.3.7 to 42.7.2.
- [Release notes](https://github.com/pgjdbc/pgjdbc/releases)
- [Changelog](https://github.com/pgjdbc/pgjdbc/blob/master/CHANGELOG.md)
- [Commits](https://github.com/pgjdbc/pgjdbc/commits)

Signed-off-by: dependabot[bot] <support@github.com>
```

## Author and Committer Patterns

### Email Conventions from Repository Analysis
- **Personal commits**: `osalvador@gmail.com`
- **Corporate commits**: `oscarsm@inditex.com` 
- **Contributor commits**: Use contributor's primary email
- **Automated commits**: `support@github.com` (Dependabot)

### Commit Author Guidelines
- Use consistent email across commits in the same PR
- Corporate contributors should use company email for work-related commits
- Open source contributors should use their preferred public email

## Special Commit Types

### Security Updates
```
build(deps): bump jackson-databind from 2.12.6.1 to 2.12.7.1

Addresses security vulnerability CVE-XXXX-XXXX
```

### Breaking Changes
```
feat!: upgrade to jdk11

BREAKING CHANGE: Minimum Java version increased from 8 to 11
Update deployment scripts and documentation accordingly
```

### Revert Commits
```
Revert "release: 0.14.0"

This reverts commit 7d2cef3a9936ca698dd37ee0c534116c60891d01.
```

## Repository-Specific Conventions

### Version Management
```
release v0.15.0           # Version release format
feat: bump version        # Version preparation commits
```

### Documentation Updates
```
doc: update readme
Update README.md          # Direct file update commits
```

### Container and Deployment
```
feat: Change version (8->9) for UBI Image
feat: Change version (8->9) for UBI Image (Fix readme + Fix Github actions)
```

### Integration with External Tools
- **Sentry integration**: Include Sentry DSN changes in commit messages
- **TestContainers**: Reference container version updates
- **GitHub Actions**: Document workflow changes

## Quality Standards

### Commit Message Quality
✅ **Good**: Clear, specific, includes context
```
fix: handle blob data from mysql to postgres
test: add cross-database replication scenarios
feat: implement bandwidth throttling for large datasets
```

❌ **Avoid**: Vague, generic, missing context
```
fix bug
update code
changes
```

### Commit Atomicity
- **One logical change per commit**: Don't mix feature additions with refactoring
- **Complete changes**: Each commit should leave the code in a working state
- **Reversible**: Each commit should be safely revertible

### Testing Requirements
- New features require corresponding test additions
- Bug fixes should include regression tests where possible
- Cross-database scenarios should be tested with TestContainers

This git commit standard ensures consistency with existing repository patterns while maintaining high code quality and clear change tracking for the ReplicaDB project.
