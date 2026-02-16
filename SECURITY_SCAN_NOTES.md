# Security Scan Notes

This document explains known false positives and security considerations for ReplicaDB dependencies.

## Microsoft JDBC Driver for SQL Server (mssql-jdbc)

### Current Version: 13.2.1.jre8

**Status**: ✅ **SECURE** - This is the latest patched version for Java 8 environments.

### Known False Positive

Some security scanning tools (e.g., Trivy, Snyk, internal scanners) may incorrectly flag `mssql-jdbc:13.2.1.jre8` as vulnerable to CVE-2025-59250, showing affected versions like:
- `>= 11.2.0.jre11, < 11.2.4.jre11`
- `>= 12.2.0.jre11, < 12.2.1.jre11`
- `>= 13.2.0.jre11, < 13.2.1.jre11`

**Why this is a false positive:**
- All listed affected versions are `.jre11` variants, NOT `.jre8` variants
- Security scanners sometimes fail to properly distinguish between jre8 and jre11 suffixes
- The vulnerability CVE-2025-59250 **IS FIXED** in 13.2.1.jre8

### Official Verification

**CVE-2025-59250** (Improper Input Validation/Spoofing):
- **Patched for JRE 8**: 13.2.1.jre8 ✅
- **Patched for JRE 11**: 13.2.1.jre11 ✅
- **Sources**: 
  - Microsoft Security Advisory
  - NVD: https://nvd.nist.gov/vuln/detail/CVE-2025-59250
  - Snyk: SNYK-JAVA-COMMICROSOFTSQLSERVER-13821835
  - CVE Record: https://www.cve.org/CVERecord?id=CVE-2025-59250
  - GitHub: https://github.com/microsoft/mssql-jdbc/issues/2831

### Why Not Upgrade to a Newer Version?

As of February 2026:
- **13.2.1.jre8** is the **latest stable version** for Java 8
- Versions 13.3.x and above only support Java 11+
- Microsoft no longer releases jre8 variants for versions after 13.2.1
- ReplicaDB uses Java 11+ (see pom.xml), but the driver is marked as `provided` scope for users who may still run on Java 8

### Recommendation

If your security scanner flags this version:
1. Verify the scanner is checking the correct variant (jre8 vs jre11)
2. Add an exception/ignore rule for this false positive
3. Reference this document and the official CVE sources listed above
4. If using Java 11+, you may optionally use the jre11 variant, but it's not required

---

## Last Updated
February 16, 2026 - Security vulnerability fixes in PR #[number]
