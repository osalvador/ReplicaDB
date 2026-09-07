---
title: Kafka connector
description: Kafka sink behavior and JSON row serialization guidance.
---

# Kafka

Use the `kafka:` scheme. Kafka is sink-only and the maintained capability is
complete-mode publishing. Rows are serialized as JSON messages; topic,
partition, key, and producer settings must match the consumer contract.

Use environment-managed client properties for TLS and authentication. Kafka is
not a table sink and does not provide complete-atomic staging; design replay,
retention, and downstream idempotency around the topic contract.