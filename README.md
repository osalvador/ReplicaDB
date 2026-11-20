# ReplicaDB

[![Latest release](https://img.shields.io/github/v/release/osalvador/ReplicaDB)](https://github.com/osalvador/ReplicaDB/releases/latest)
[![Apache 2.0 license](https://img.shields.io/github/license/osalvador/ReplicaDB)](LICENSE)

**Copy data in batches between databases and other data stores.** Use
ReplicaDB to migrate an Oracle table to PostgreSQL, refresh a reporting table,
or export selected rows to CSV. ReplicaDB can split large transfers into
parallel tasks where the connectors support it. It runs without installing
agents or triggers on the source database.

## Choose CLI or Server

| If you want to… | Start here |
| --- | --- |
| Run a transfer from a terminal, script, or existing scheduler | **CLI** · Run a standalone command or save a reusable options file. [CLI quickstart](https://osalvador.github.io/ReplicaDB/getting-started/cli-quickstart/) |
| Manage recurring transfers with a team | **Server** · Configure jobs in the web UI or API, schedule runs, manage access, and inspect run history. [Server quickstart](https://osalvador.github.io/ReplicaDB/getting-started/server-quickstart/) |

Both use the same replication engine. The two quickstarts use the same
PostgreSQL sample data so you can try either workflow.

## Try a local transfer

Copy a single order from SQLite to CSV. You need Java 17 or newer and
`sqlite3`. Run this in a POSIX shell. It creates a fresh directory for the
database and output file.

```bash
demo_dir=$(mktemp -d /tmp/replicadb-demo.XXXXXX)
cd "$demo_dir"

curl -fLO https://github.com/osalvador/ReplicaDB/releases/download/v1.0.3/ReplicaDB-1.0.3.tar.gz
tar -xzf ReplicaDB-1.0.3.tar.gz

sqlite3 orders.db "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT);
INSERT INTO orders VALUES (1, 'pending');"

cat > orders.conf <<'EOF'
mode=complete
jobs=1
source.connect=jdbc:sqlite:orders.db
source.table=orders
sink.connect=file://${PWD}/orders.csv
sink.file.format=csv
sink.connect.parameter.format.firstRecordAsHeader=true
EOF

./bin/replicadb --options-file orders.conf
cat orders.csv
```

Expected output:

```csv
id,status
1,pending
```

For a database-to-database transfer, follow the
[PostgreSQL walkthrough](https://osalvador.github.io/ReplicaDB/getting-started/cli-quickstart/).
Use the [options-file guide](https://osalvador.github.io/ReplicaDB/cli/configuration/)
to save a repeatable transfer. For Windows packages or the full installation
options, see [Releases](https://github.com/osalvador/ReplicaDB/releases/latest),
[CLI installation](https://osalvador.github.io/ReplicaDB/cli/installation/),
or [Server installation](https://osalvador.github.io/ReplicaDB/server/installation/).
The [CLI container image](https://hub.docker.com/r/osalvador/replicadb) is also
available for Docker and Podman.

## Choose how to write to the destination

| Mode | What it does |
| --- | --- |
| `complete` | Replaces the destination by default. An interrupted run can leave a database destination empty or partially loaded. |
| `complete-atomic` | Loads a staging table before publishing the replacement. The destination connector determines the transaction guarantees. |
| `incremental` | Writes a selected set of new or changed rows. Supported database sinks merge by key, while file sinks append. |

See [replication modes](https://osalvador.github.io/ReplicaDB/cli/replication-modes/)
for what readers see during a run, how watermarks advance, and how to recover
from an interruption.

## Compatible databases

Connectors cover Oracle, PostgreSQL, MySQL, MariaDB, SQL Server, Db2, SQLite,
MongoDB, Denodo, CSV, Amazon S3, Kafka, and generic JDBC. Source and destination
roles, modes, and staging support vary by connector. Check the
[connector matrix](https://osalvador.github.io/ReplicaDB/connectors/) before
choosing a pair.

## Documentation

- [Configuration wizard](https://osalvador.github.io/ReplicaDB/wizard/index.html) to build a CLI options file.
- [Filtering and queries](https://osalvador.github.io/ReplicaDB/cli/filtering-and-queries/) and [multi-table replication](https://osalvador.github.io/ReplicaDB/cli/multi-table/) to select what a transfer copies.
- [Server operations](https://osalvador.github.io/ReplicaDB/operations/) and the [Server API](https://osalvador.github.io/ReplicaDB/api-introduction/) to deploy, operate, and automate managed jobs.

See the [documentation portal](https://osalvador.github.io/ReplicaDB/) for the
complete guides.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned capabilities and product decisions.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build and test instructions.
Use [GitHub Issues](https://github.com/osalvador/ReplicaDB/issues) to report a
bug or propose a feature.

## License

ReplicaDB is available under the [Apache License 2.0](LICENSE).
