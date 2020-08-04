#!/bin/bash

SETUP_PATH="./tests/setUp"
TEARDOWN_PATH="./tests/tearDown"

SOURCE_DB="pg"
SINK_DB="pg"
CURRENT_PATH="./tests/${SOURCE_DB}2${SINK_DB}"
TOTAL_NUM_ROWS=10000

SOURCE_CONNECT="pg://replicadb:replicadb@localhost/replicadb?sslmode=disable"
SINK_CONNECT="pg://replicadb:replicadb@localhost/replicadb?sslmode=disable"

setUp(){    
    usql ${SOURCE_CONNECT} -f ${SETUP_PATH}"/${SOURCE_DB}-source.sql"
    usql ${SINK_CONNECT} -f ${SETUP_PATH}"/${SINK_DB}-sink.sql"
}

tearDown(){
    usql ${SOURCE_CONNECT} -f ${TEARDOWN_PATH}"/${SOURCE_DB}-source.sql"
    usql ${SINK_CONNECT} -f ${TEARDOWN_PATH}"/${SINK_DB}-sink.sql"
}

sinkTableCountRows(){
    numRowsSinkTable=$(usql ${SINK_CONNECT} -c 'select count(*) from public.t_sink;' -C | tail -n 2 | head -n 1)    
    echo ${numRowsSinkTable}
}

# Tests
testComplete(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}

testCompleteAtomic(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete-atomic
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}

testIncremental(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode incremental
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}

# Test with 4 Jobs
testCompleteMultipleJobs(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete --jobs 4
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}

testCompleteAtomicMultipleJobs(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete-atomic --jobs 4
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}

testIncrementalMultipleJobs(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode incremental --jobs 4
    assertTrue 'Replication failed' "[ $? -eq 0 ]"
    assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
}


# Load shUnit2.
. ./tests/shunit2