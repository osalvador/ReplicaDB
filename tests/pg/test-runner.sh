#!/bin/bash

export INTEGRATION_TEST_PATH="./tests"

oneTimeSetUp(){    
    export TEST_PATH="${INTEGRATION_TEST_PATH}/pg"
    export SETUP_PATH="${TEST_PATH}/setUp"
    export TEARDOWN_PATH="${TEST_PATH}/tearDown"
}

testPg2Pg(){
    ${TEST_PATH}/pg2pg/run-test.sh
}

testPg2Sqlserver(){
    ${TEST_PATH}/pg2sqlserver/run-test.sh
}

# Load shUnit2.
. ${INTEGRATION_TEST_PATH}/shunit2