#!/bin/bash

CURRENT_PATH="./tests/pg2pg"

# setup
./usql pg://replicadb:replicadb@localhost/replicadb?sslmode=disable -f ${CURRENT_PATH}"/db-setup.sql"

# Tests
./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete
./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete-atomic
./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode incremental

# teardown
./usql pg://replicadb:replicadb@localhost/replicadb?sslmode=disable -f ${CURRENT_PATH}"/db-teardown.sql"