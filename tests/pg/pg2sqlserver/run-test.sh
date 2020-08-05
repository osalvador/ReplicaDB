#!/bin/bash

oneTimeSetUp(){
    SOURCE_DB="pg"
    SINK_DB="sqlserver"
    CURRENT_PATH="${TEST_PATH}/${SOURCE_DB}2${SINK_DB}"
    TOTAL_NUM_ROWS=1000

    export REPLICADB_PASSWROD="ReplicaDB_1234"
    export REPLICADB_USER="replicadb"
    SOURCE_CONNECT="pg://${REPLICADB_USER}:${REPLICADB_PASSWROD}@localhost/replicadb?sslmode=disable"
    SINK_CONNECT="ms://sa:${REPLICADB_PASSWROD}@localhost/master"

    usql ${SOURCE_CONNECT} -f ${SETUP_PATH}"/${SOURCE_DB}-source.sql"
}

oneTimeTearDown(){
    usql ${SOURCE_CONNECT} -f ${TEARDOWN_PATH}"/${SOURCE_DB}-source.sql"
}

setUp(){    
    usql ${SINK_CONNECT} -f ${SETUP_PATH}"/${SINK_DB}-sink.sql"
}
tearDown(){    
    usql ${SINK_CONNECT} -f ${TEARDOWN_PATH}"/${SINK_DB}-sink.sql"
}

sinkTableCountRows(){
    numRowsSinkTable=$(usql ${SINK_CONNECT} -c 'select count(*) from dbo.t_sink;' -C | tail -n 2 | head -n 1)    
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
    assertTrue 'Replication must fail' "[ $? -eq 0 ]"
    #assertEquals ${TOTAL_NUM_ROWS} $(sinkTableCountRows)
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
. ${INTEGRATION_TEST_PATH}/shunit2