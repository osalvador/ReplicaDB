---
title: Kafka connector
description: Kafka sink behavior and JSON row serialization guidance.
---


Kafka is sink-only. Use the `kafka:` scheme in the sink connection URL, for
example `kafka://${KAFKA_BOOTSTRAP_SERVERS}`. The bootstrap servers are taken
from the URL after `kafka://`. The only supported sink mode is `complete`, which
publishes every source row as one message.

## Sink connection parameters

Kafka-specific settings are passed as `sink.connect.parameter.*` properties.

| Parameter | Required | Behavior |
| --- | --- | --- |
| `sink.connect.parameter.topic` | Yes | Topic that receives every message. |
| `sink.connect.parameter.partition` | No | Integer partition number. When set, every record is sent to that partition. When omitted, the Kafka producer chooses the partition. |
| `sink.connect.parameter.key` | No | Fixed producer key sent with every record. When omitted, records have no key. |

Any other `sink.connect.parameter.*` name is passed to the Kafka producer
configuration, so producer settings such as `security.protocol` can be set the
same way. Keep credentials in environment-managed values rather than in the
properties file.

## Message format

Key and value use string serialization. Each row is written as a JSON object
keyed by the sink column names:

- A single sink column named `json` is sent as the raw JSON value of that
  column.
- Timestamps are written as UTC text in the `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`
  format.
- Binary columns are written as raw byte values in the JSON object.

Validate the resulting schema with the consumer before a large publish.

## Example properties file

```properties
mode=complete
jobs=1

source.connect=jdbc:postgresql://${PGHOST}:5432/${PGDATABASE}
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=public.orders

sink.connect=kafka://${KAFKA_BOOTSTRAP_SERVERS}
sink.connect.parameter.topic=orders
sink.connect.parameter.partition=0
sink.connect.parameter.key=orders-export
```

## Limits and notes

- Kafka is a sink only. It cannot be used as a source.
- Kafka is not a table sink. It has no staging table and no complete-atomic
  mode, and it does not support incremental mode.
- A failed run does not retract messages already acknowledged by the broker.
  Design replay, topic retention, and downstream idempotency around the topic
  contract.
- Without an explicit partition or key, ordering is limited to what the Kafka
  producer configuration provides.
