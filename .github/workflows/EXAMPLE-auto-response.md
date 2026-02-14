# Example: Auto-Response for Installation Issue

## Sample Issue
**Title:** "How to install ReplicaDB?"
**Body:** "I want to install ReplicaDB to replicate data from PostgreSQL to MySQL"

---

## Auto-Generated Response

👋 Hello @user! Thank you for opening this issue.

🤖 **Automated Initial Response**

### 📦 Installation

ReplicaDB requires Java 11 or higher. You can install it in the following ways:

**Direct download:**
```bash
curl -o ReplicaDB-0.18.0.tar.gz -L "https://github.com/osalvador/ReplicaDB/releases/download/v0.18.0/ReplicaDB-0.18.0.tar.gz"
tar -xvzf ReplicaDB-0.18.0.tar.gz
./bin/replicadb --help
```

**Docker:**
```bash
docker pull osalvador/replicadb:latest
docker run -it osalvador/replicadb:latest --help
```

### 🗄️ Supported Databases

ReplicaDB supports a wide range of databases:
- **Relational:** Oracle, PostgreSQL, MySQL/MariaDB, SQL Server, SQLite, IBM DB2 (LUW and AS/400)
- **NoSQL:** MongoDB
- **Others:** Denodo (data virtualization), CSV, Amazon S3, Kafka
- **Any JDBC-compliant database** (with some limitations)

For database-specific information, check the [complete documentation](https://osalvador.github.io/ReplicaDB/docs/docs.html).

### 📚 Additional Resources

- 📖 [Complete documentation](https://osalvador.github.io/ReplicaDB/docs/docs.html)
- 🚀 [Quick start guide](https://github.com/osalvador/ReplicaDB#installation)
- 🏗️ [Architecture decisions](https://github.com/osalvador/ReplicaDB/blob/master/ARCHITECTURE_DECISIONS.md)
- 🤝 [Contributing guide](https://github.com/osalvador/ReplicaDB/blob/master/CONTRIBUTING.md)
- 💬 [GitHub Discussions](https://github.com/osalvador/ReplicaDB/discussions)

---

*This response was automatically generated based on detected topics: installation, database.*

If you need more specific help, please provide:
- ReplicaDB version you are using
- Source and sink databases (type and version)
- Command or configuration file you are running
- Complete error messages if any

A human maintainer will review your issue soon! 🙂

---

## Labels Applied

- `auto-responded` (automated response tracking)
- `question` (detected from "How to" in title)

---

# Example: Auto-Response for Bug Report

## Sample Issue
**Title:** "Error connecting to Oracle database"
**Body:** "I'm getting an exception when trying to connect to Oracle: Connection failed with error ORA-12154"

---

## Auto-Generated Response

👋 Hello @user! Thank you for opening this issue.

🤖 **Automated Initial Response**

### 🗄️ Supported Databases

ReplicaDB supports a wide range of databases:
- **Relational:** Oracle, PostgreSQL, MySQL/MariaDB, SQL Server, SQLite, IBM DB2 (LUW and AS/400)
- **NoSQL:** MongoDB
- **Others:** Denodo (data virtualization), CSV, Amazon S3, Kafka
- **Any JDBC-compliant database** (with some limitations)

For database-specific information, check the [complete documentation](https://osalvador.github.io/ReplicaDB/docs/docs.html).

### 🔍 Troubleshooting

If you encounter errors:

1. **Verify connectivity:** Ensure both databases are accessible
2. **Check permissions:** 
   - Source database: requires SELECT permissions
   - Sink database: requires INSERT/UPDATE/DELETE/CREATE TABLE permissions
3. **JDBC drivers:** Ensure you have the correct drivers in `$REPLICADB_HOME/lib/`
4. **Logs:** Check the logs for specific error details
5. **Data types:** Some conversions between databases may have limitations

### 📚 Additional Resources

- 📖 [Complete documentation](https://osalvador.github.io/ReplicaDB/docs/docs.html)
- 🚀 [Quick start guide](https://github.com/osalvador/ReplicaDB#installation)
- 🏗️ [Architecture decisions](https://github.com/osalvador/ReplicaDB/blob/master/ARCHITECTURE_DECISIONS.md)
- 🤝 [Contributing guide](https://github.com/osalvador/ReplicaDB/blob/master/CONTRIBUTING.md)
- 💬 [GitHub Discussions](https://github.com/osalvador/ReplicaDB/discussions)

---

*This response was automatically generated based on detected topics: database, troubleshooting.*

If you need more specific help, please provide:
- ReplicaDB version you are using
- Source and sink databases (type and version)
- Command or configuration file you are running
- Complete error messages if any

A human maintainer will review your issue soon! 🙂

---

## Labels Applied

- `auto-responded` (automated response tracking)
- `bug` (detected from "Error", "exception", "failed" keywords)
