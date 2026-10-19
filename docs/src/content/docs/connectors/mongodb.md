---
title: MongoDB connector
description: MongoDB and MongoDB SRV source/sink capabilities and caveats.
---


The MongoDB connector reads and writes collections using the `mongodb:` and
`mongodb+srv:` schemes. It supports all three source modes as a source, and
complete or incremental modes as a sink. The sink does not support
complete-atomic mode.

## Connection strings

The `source.connect` and `sink.connect` values follow the
[MongoDB connection string format](https://www.mongodb.com/docs/manual/reference/connection-string/):

| Deployment | Pattern |
|------------|---------|
| Self-managed | `mongodb://${MONGO_HOST}:27017/catalog` |
| MongoDB Atlas | `mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority` |

The database name in the URI (`catalog` above) is required. Supply credentials
through the connection string from environment-managed configuration, such as
`${MONGO_USER}` and `${MONGO_PASSWORD}`, and never commit literal values.

## Source options

| Parameter | Description |
|-----------|-------------|
| `source.connect` | MongoDB connection string, including the database name |
| `source.table` | Collection to read |
| `source.columns` | BSON projection applied to the collection, in JSON format, for example `{_id:0,sku:1}` |
| `source.where` | BSON query filter applied to the collection, in JSON format |
| `source.query` | Aggregation pipeline executed on the collection, as a JSON array |

Without `source.query`, ordinary reads sort by `_id` in ascending order. Validate
the ordering and the cost of the pipeline on the actual collection.

## Sink options

| Parameter | Description |
|-----------|-------------|
| `sink.connect` | MongoDB connection string, including the database name |
| `sink.table` | Collection to write |
| `sink.columns` | Mapping from source fields to sink fields, used in examples 4 and 5 |

## Field mapping

- MongoDB-to-MongoDB transfers keep each source document.
- Relational source rows become documents. Field names are normalized through
  the primary-key normalization map when the column is a key, and lowercased
  otherwise.
- PostgreSQL `jsonb` values are converted to a MongoDB document, or to a string
  when they are not an object.
- Test nested, null, binary, and temporal fields on a disposable collection
  before production use.

## Incremental replication

The incremental sink writes with the `$merge` stage. The `$merge` stage requires
MongoDB 4.2 or later, per the MongoDB documentation.

- The merge key is the first user-defined unique-index field of the sink
  collection. If there is none, ReplicaDB falls back to `_id`.
- If the sink lacks the unique-index, ReplicaDB creates one when needed.
- Staged documents with a missing or null merge key cause the run to fail.
- When `_id` is the merge key, every source document must carry `_id`. Keep
  `_id` in `source.columns` or in the `source.query` projection. A run where
  `_id` is missing fails.
- Relational sources must expose an `_id` column when `_id` is the merge key.
- Incremental merge does not propagate source deletes. Deleted source documents
  remain in the sink.
- `sink.auto.create` is a SQL DDL feature and is not supported for MongoDB
  collections.

## Examples

1. Replicate all documents from a self-managed MongoDB collection to a MongoDB Atlas collection:

```properties
######################## ReplicaDB General Options ########################
mode=complete
jobs=1
fetch.size=100
############################# Source Options ##############################
source.connect=mongodb://${MONGO_HOST}:27017/catalog
source.table=products
############################# Sink Options ################################
sink.connect=mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority
sink.table=products
```

2. Replicate a subset of documents, using `source.query` to set a MongoDB aggregation pipeline:

```properties
######################## ReplicaDB General Options ########################
mode=complete
jobs=1
fetch.size=100
############################# Source Options ##############################
source.connect=mongodb://${MONGO_HOST}:27017/catalog
source.table=products
source.query=[{ $match : {startDate:{$gte:ISODate("2021-01-01T00:00:00.000Z")}} } \
  ,{$sort:{startDate:1}} \
  ,{$project: {_id:0, shopId:1, productId:1, price:1, startDate:1} } ]
############################# Sink Options ################################
sink.connect=mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority
sink.table=products
```

3. Replicate a table from a PostgreSQL database to a MongoDB Atlas collection, renaming columns with `source.columns`:

```properties
######################## ReplicaDB General Options ########################
mode=complete
jobs=1
fetch.size=100
verbose=true
############################# Source Options ##############################
source.connect=jdbc:postgresql://${PGHOST}:5432/postgres
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=t_roles
source.columns=id as "rolId", txt_code as code, txt_role_name as "roleName" \
  , cod_v_usuario_creacion as "createdBy", fec_dt_creacion as "createdAt" \
  , cod_v_usuario_modificacion as "updatedBy", fec_dt_modificacion as "updatedAt"
############################# Sink Options ################################
sink.connect=mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority
sink.table=roles
```

4. Replicate a MongoDB Atlas collection to a PostgreSQL table. Both `source.columns` and `sink.columns` are required to match the source fields to the sink columns:

```properties
######################## ReplicaDB General Options ########################
mode=complete
jobs=1
fetch.size=100
verbose=true
############################# Source Options ##############################
source.connect=mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority
source.table=roles
source.columns={_id:0,rolId:1,roleName:1,createdBy:1,createdAt:1,updatedBy:1,updatedAt:1}
source.where={createdAt:{$gte:ISODate("2021-01-01T00:00:00.000Z")}}
############################# Sink Options ################################
sink.connect=jdbc:postgresql://${PGHOST}:5432/postgres
sink.user=${PGUSER}
sink.password=${PGPASSWORD}
sink.table=t_roles
sink.columns=id, txt_role_name, cod_v_usuario_creacion, fec_dt_creacion, cod_v_usuario_modificacion, fec_dt_modificacion
```

5. Replicate a MongoDB Atlas collection to PostgreSQL, projecting selected fields with `source.columns`. The projection is optional; without it, every field of each document is read:

```properties
######################## ReplicaDB General Options ########################
mode=complete
jobs=1
fetch.size=100
verbose=true
############################# Source Options ##############################
source.connect=mongodb+srv://${MONGO_ATLAS_HOST}/catalog?retryWrites=true&w=majority
source.table=products
source.columns={_id:0,sku:1,name:1}
############################# Sink Options ################################
sink.connect=jdbc:postgresql://${PGHOST}:5432/postgres
sink.user=${PGUSER}
sink.password=${PGPASSWORD}
sink.table=products_slim
sink.columns=sku, name
```

