---
applyTo: '**'
---

# ReplicaDB: Git Commit Message Standards

## Commit Message Analysis

Based on analysis of actual commit history in the ReplicaDB repository, the project follows **pragmatic, descriptive commit messages** focused on clarity over ceremony.

## Actual Commit Patterns (Evidence-Based)

### Pattern Analysis from Recent Commits

The repository demonstrates the following **consistent patterns**:

1. **Imperative mood**: "Fix", "Add", "Update", "Remove", "Convert"
2. **Technical specificity**: Reference exact types, methods, or components
3. **Problem-focused**: Describe the issue being solved, not implementation details
4. **Direct**: No ticket references, no conventional commit prefixes

### Real Examples from Commit History

```
✓ Fix binary column handling for all SQL Server binary types
✓ Fix VARBINARY bulk copy: convert hex strings to byte arrays  
✓ Convert SQLXML to string for storage as CLOB in SQL Server
✓ Skip Oracle INTERVAL and SQLXML types instead of converting to strings
✓ Add streaming LOB support for SQL Server bulk copy
✓ Update Dockerfile base image from openjdk:8 to eclipse-temurin:11
✓ Add automated release script and documentation
✓ Release v0.16.0
```

### Anti-Patterns (NOT Used in This Repository)

```
✗ feat: add support for SQL Server bulk copy
✗ fix(sqlserver): handle VARBINARY columns [TICKET-123]
✗ chore: update dependencies
✗ Fixed bug with null handling
```

## Commit Message Standards

### Format

```
<action> <specific technical detail>

Optional body with:
- Additional context
- Related changes
- Breaking changes
```

### Action Verbs (From Actual Usage)

| Verb | Usage | Example |
|------|-------|---------|
| **Fix** | Resolve bugs, correct behavior | Fix precision handling for VARCHAR vs NUMERIC columns |
| **Add** | New features, new capabilities | Add streaming LOB support for SQL Server bulk copy |
| **Update** | Modify existing code/docs | Update README.md version update to release script |
| **Remove** | Delete code or features | Remove implementation_plan.md and add to .gitignore |
| **Convert** | Transform data or logic | Convert non-hex strings to hex for VARBINARY columns |
| **Skip** | Deliberately avoid processing | Skip Oracle INTERVAL and SQLXML types |
| **Detect** | Add detection logic | Detect and preserve already-hex strings |
| **Improve** | Performance or quality enhancement | Improve JDBC type handling for negative type codes |
| **Optimize** | Performance-focused change | Optimize getRowData logic |
| **Cap** | Limit values | Cap precision at 38 for SQL Server bulk copy compatibility |
| **Release** | Version tagging | Release v0.16.0 |

### Component-Specific Patterns

#### Database Manager Changes
```
Fix SQL Server bulk copy type and metadata handling
Add streaming LOB support for SQL Server bulk copy
Improve JDBC type handling for negative type codes
```

#### Type Handling
```
Fix precision handling for VARCHAR vs NUMERIC columns
Handle unsupported and Oracle-specific SQL types
Convert SQLXML to string for storage as CLOB
```

#### Test Changes
```
Fix MariaDB2S3FileTest assertions and clean up logging
Add CSV row count assertions to MariaDB2S3FileTest
```

#### Infrastructure
```
Update Dockerfile base image from openjdk:8 to eclipse-temurin:11
Add workflow_dispatch trigger to CI_Release workflow
Fix CI_Release workflow to use Java 11 instead of Java 8
```

#### Release Management
```
Release v0.16.0
Add automated release script and documentation
Add README.md version update to release script
```

## Writing Effective Commit Messages

### Do ✓

1. **Be specific about what changed**
   ```
   Fix VARBINARY bulk copy: convert hex strings to byte arrays
   ```
   (Not: "Fix bulk copy issues")

2. **Name database types and technical components**
   ```
   Skip Oracle INTERVAL and SQLXML types instead of converting to strings
   ```

3. **Describe the solution approach when relevant**
   ```
   Detect and preserve already-hex strings for VARBINARY columns
   ```

4. **Group related changes logically**
   ```
   Fix MariaDB2S3FileTest assertions and clean up logging
   ```

5. **Use technical accuracy**
   ```
   Cap precision at 38 for SQL Server bulk copy compatibility
   ```

### Don't ✗

1. **Avoid vague descriptions**
   ```
   ✗ Fixed some bugs
   ✗ Updated code
   ✗ Various improvements
   ```

2. **Don't use conventional commit prefixes**
   ```
   ✗ feat: new feature
   ✗ fix: bug fix
   ✗ chore: maintenance
   ```

3. **Don't include ticket numbers**
   ```
   ✗ [JIRA-123] Fix SQL Server issue
   ```

4. **Avoid past tense**
   ```
   ✗ Fixed the bug
   ✗ Added new feature
   ```

5. **Don't be too brief to be unclear**
   ```
   ✗ Fix tests
   ✗ Update config
   ```

## Multi-Change Commits

When commits address multiple related issues, use " and " to connect them:

```
Fix CSV row counting in MariaDB2S3FileTest - count all files and clean bucket before tests
Add CSV row count assertions to MariaDB2S3FileTest and clean up orphaned LOG fields
```

## Phase-Based Development

The project uses phase markers for major refactoring:

```
FASE 3: Code cleanup - remove unused code
```

(Note: Use sparingly, only for significant architectural work)

## Release Commits

Release commits follow a simple pattern:

```
Release v0.16.0
```

No additional description needed - release notes are in RELEASE_GUIDE.md.

## Body Guidelines

Most commits are single-line. Add body text when:

1. **Change requires explanation**
2. **Breaking changes introduced**
3. **Multiple related files affected**

Format:
```
Fix SQL Server bulk copy type and metadata handling

- Updated column metadata extraction to handle negative type codes
- Added precision capping for SQL Server compatibility
- Fixed type mapping for VARBINARY and SQLXML
```

## Quality Checklist

Before committing, ask:

- [ ] Does the message describe **what** changed specifically?
- [ ] Could another developer understand the change from the message alone?
- [ ] Are technical terms (types, methods, databases) named correctly?
- [ ] Is it in imperative mood ("Fix" not "Fixed")?
- [ ] Is it focused on one logical change?

## Integration with Release Process

Commit messages are not used for changelog generation. The project uses:
- `RELEASE_GUIDE.md` for release documentation
- `release.sh` script for automated releases
- GitHub Actions for CI/CD

Therefore, commit messages prioritize **developer clarity** over **automated parsing**.

## Examples by Category

### Bug Fixes
```
Fix binary column handling for all SQL Server binary types
Fix VARBINARY bulk copy: convert hex strings to byte arrays
Fix precision handling for VARCHAR vs NUMERIC columns
Fix invalid XML comment in log4j2.xml
Fix CI_Release workflow to use Java 11 instead of Java 8
```

### New Features
```
Add streaming LOB support for SQL Server bulk copy
Add automated release script and documentation
Add workflow_dispatch trigger to CI_Release workflow
Add CSV row count assertions to MariaDB2S3FileTest
```

### Improvements
```
Improve JDBC type handling for negative type codes
Improve SQL Server bulk copy column mapping and precision
Optimize getRowData logic
```

### Data/Type Handling
```
Convert SQLXML to string for storage as CLOB in SQL Server
Convert non-hex strings to hex for VARBINARY columns
Detect and preserve already-hex strings for VARBINARY columns
Handle non-hex string data in VARBINARY columns for SQL Server bulk copy
Skip Oracle INTERVAL and SQLXML types instead of converting to strings
Cap precision at 38 for SQL Server bulk copy compatibility
```

### Updates
```
Update README.md
Update Dockerfile base image from openjdk:8 to eclipse-temurin:11
Add README.md version update to release script
```

### Removals
```
Remove implementation_plan.md and add to .gitignore
```

### Code Cleanup
```
FASE 3: Code cleanup - remove unused code
Fix MariaDB2S3FileTest assertions and clean up logging
Add CSV row count assertions to MariaDB2S3FileTest and clean up orphaned LOG fields
```

## Summary

ReplicaDB commit messages prioritize:
1. **Technical specificity** over generic descriptions
2. **Problem-solution clarity** over ticket tracking
3. **Developer communication** over automated tooling
4. **Imperative mood** consistently
5. **Direct language** without ceremony

This style aligns with the project's pragmatic, enterprise-focused approach to data replication.
