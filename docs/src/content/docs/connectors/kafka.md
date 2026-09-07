---
title: Kafka connector
description: Kafka sink behavior and JSON row serialization guidance.
---

# Kafka

Use the `kafka:` scheme. Kafka is sink-only and the maintained capability is
complete-mode publishing. Rows are serialized as JSON messages; topic,
partition, key, and producer settings must match the consumer contract.

Set the required topic with `sink.connect.parameter.topic`. An optional
`sink.connect.parameter.partition` routes every record to one numeric
partition, and `sink.connect.parameter.key` supplies one fixed producer key.
Without an explicit partition, Kafka's producer decides placement. Without a
key, ordering is only the ordering provided by the selected partition and
producer configuration.

ReplicaDB serializes each row as a JSON object using sink column names. A
single column named `json` is parsed as an existing JSON object; other rows are
converted by JDBC type, with UTC timestamp text and binary content encoded for
JSON. Validate the resulting schema with the consumer before a large publish.

Use environment-managed client properties for TLS and authentication. Kafka is
not a table sink and does not provide complete-atomic staging; design replay,
retention, and downstream idempotency around the topic contract. A failed run
does not retract messages already acknowledged by the broker.
