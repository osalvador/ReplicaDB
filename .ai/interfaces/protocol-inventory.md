---
type: Protocol Inventory
description: The core exposes source and sink adapters for databases, files, object storage, and Kafka; the managed server currently has no event-bus or gRPC interface.
sources:
  - id: managers
    resource: src/main/java/org/replicadb/manager
  - id: kafka
    resource: src/main/java/org/replicadb/manager/KafkaManager.java
  - id: files
    resource: src/main/java/org/replicadb/manager/file/FileManager.java
  - id: server
    resource: replicadb-server/src/main/java/org/replicadb/server
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Kafka is implemented as a core sink adapter, not as a server event consumer or producer. Files and object storage are likewise core transfer endpoints. SQL/JDBC, MongoDB, DB2, Denodo, S3, Kafka, local-file, CSV, and ORC paths are selected through manager factories and row-set adapters.

No `.proto`, AsyncAPI contract, or server event-consumer package was found in the selected source tree. Managed scheduling uses Quartz and PostgreSQL state rather than a message-broker interface. Phase 3.1 adds durable PostgreSQL claim/recovery state, not a wire protocol. `LISTEN/NOTIFY`, polling dispatch, and remote cancellation remain approved but unimplemented Phase 3.2 work and should not be described as an implemented event interface.

Reference implementations: `src/main/java/org/replicadb/manager/ManagerFactory.java`, `KafkaManager.java`, and `replicadb-server/src/main/java/org/replicadb/server`.
