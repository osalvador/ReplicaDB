---
title: CLI quickstart
description: Install ReplicaDB and run a first standalone replication workflow.
---

# CLI quickstart

This path runs one direct transfer from a release archive. It leaves metadata
ownership, scheduling, and process supervision to your existing automation.

## Install the archive

The CLI needs Java 17 or newer. Download the release archive, extract it, and
check the launcher before preparing a replication command:

```bash
curl -fL -o ReplicaDB-1.0.0.tar.gz "https://github.com/osalvador/ReplicaDB/releases/download/v1.0.0/ReplicaDB-1.0.0.tar.gz"
tar -xzf ReplicaDB-1.0.0.tar.gz
cd ReplicaDB-1.0.0
./bin/replicadb --help
```

Keep this installation under its own `REPLICADB_HOME`. The CLI does not use
the managed server home or its metadata database.

## Prepare an options file

Use environment-managed connection values rather than placing credentials in
the file. The options-file contract accepts property substitutions and command
line arguments override values from the file.

```bash
export SOURCE_CONNECT
export SOURCE_TABLE
export SINK_CONNECT
export SINK_TABLE
cat > replicadb.conf <<'EOF'
mode=complete
jobs=1
source.connect=${SOURCE_CONNECT}
source.table=${SOURCE_TABLE}
sink.connect=${SINK_CONNECT}
sink.table=${SINK_TABLE}
EOF
./bin/replicadb --options-file ./replicadb.conf
```

Add connector-specific security properties according to the
[configuration guide](/ReplicaDB/cli/configuration/), keeping their values in
your environment or secret manager. Do not commit the options file when it
contains resolved values.

## Choose a mode deliberately

`complete` copies the source into a freshly prepared sink. Use
`complete-atomic` when the connector supports staging and the sink must remain
available during replacement. Use `incremental` with an explicit source filter
or watermark; it does not infer deletes or resume a cancelled transfer.

Continue with [CLI installation details](/ReplicaDB/cli/installation/),
[CLI configuration](/ReplicaDB/cli/configuration/),
[CLI troubleshooting](/ReplicaDB/cli/troubleshooting/), and
[security operations](/ReplicaDB/operations/security-and-tls/).