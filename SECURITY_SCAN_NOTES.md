# Security Scan Notes

This document explains security considerations and dependency choices for ReplicaDB.

## Microsoft JDBC Driver for SQL Server (mssql-jdbc)

### Current Version: 13.2.1.jre11

**Status**: ✅ **SECURE** - This is the latest patched version for Java 11 environments.

### Why jre11 variant?

ReplicaDB is built with **Java 11** (see `maven.compiler.source=11` and `maven.compiler.target=11` in pom.xml), so we use the **jre11 variant** of the JDBC driver to match the project's Java version.

### Security Fix

**CVE-2025-59250** (Improper Input Validation/Spoofing):
- **Severity**: High (CVSS 8.1)
- **Issue**: Hostname spoofing through improper SSL certificate validation
- **Patched Version**: 13.2.1.jre11 ✅
- **Previous Version**: 7.2.2.jre8 (vulnerable)

**Official Sources**:
- Microsoft Security Advisory
- NVD: https://nvd.nist.gov/vuln/detail/CVE-2025-59250
- Snyk: SNYK-JAVA-COMMICROSOFTSQLSERVER-13821835
- CVE Record: https://www.cve.org/CVERecord?id=CVE-2025-59250

### Version Selection Rationale

- **Why 13.2.1?** Latest stable version with CVE-2025-59250 fix
- **Why jre11?** Matches project's Java 11 compiler target
- **Scope**: `provided` - allows users to provide their own driver version if needed

### Verification

Run `mvn dependency:tree | grep mssql-jdbc` to verify:
```
com.microsoft.sqlserver:mssql-jdbc:jar:13.2.1.jre11:compile
```

---

## Last Updated
February 16, 2026 - Changed from jre8 to jre11 variant to match Java 11 target

