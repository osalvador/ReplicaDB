---
title: MongoDB connector
description: MongoDB and MongoDB SRV source/sink capabilities and caveats.
---


Use `mongodb:` or `mongodb+srv:`. MongoDB supports source and sink roles for
all three source modes and complete or incremental sink modes. The sink does
not support complete-atomic mode. It is a document sink, not a transactional
table staging target.

## Source selection

`source.table` names the collection. `source.where` accepts a BSON filter,
`source.columns` accepts a BSON projection, and `source.query` accepts a JSON
array representing an aggregation pipeline. Parallel reads add skip/limit
boundaries, and ordinary collection reads sort by object ID; validate ordering
and pipeline cost on the actual collection.

## Field mapping and writes

MongoDB-to-MongoDB transfers retain each source document. Relational source
rows become documents whose field names are normalized for sink keys and
lowercased otherwise. Primitive, temporal, binary, BLOB/CLOB, and PostgreSQL
JSON-like values take connector-specific conversion paths, so test nested,
null, binary, and mixed-type fields before production.

Sink writes use bulk operations. Incremental merge discovers unique-index
fields for identity and rejects staged documents with missing or null merge
keys. It does not propagate source deletes. `sink.auto.create` is a SQL DDL
feature and is not supported for MongoDB collections.

SRV discovery, TLS, and authentication are properties of the deployment and
connection string. Resolve security through the runtime credential chain and
keep it out of committed examples.
