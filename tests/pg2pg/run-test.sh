#!/bin/bash

CURRENT_PATH="./tests/pg2pg"

setUp(){    
    usql pg://replicadb:replicadb@localhost/replicadb?sslmode=disable -f ${CURRENT_PATH}"/db-setup.sql"
}

tearDown(){
    usql pg://replicadb:replicadb@localhost/replicadb?sslmode=disable -f ${CURRENT_PATH}"/db-teardown.sql"    
}

# Tests
testComplete(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete
}

testCompleteAtomic(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode complete-atomic
}

testIncremental(){
    ./bin/replicadb --options-file ${CURRENT_PATH}"/replicadb.conf" --mode incremental
}

# Load shUnit2.
. ./tests/shunit2