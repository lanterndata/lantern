#!/usr/bin/env bash

# Get current test file name
TESTFILE_NAME=${PGAPPNAME##pg_regress/}

if [ "$PARALLEL" -eq 0 ]; then
    # Set different name for each test database
    # As pg_regress does not support cleaning db after each test
    TEST_CASE_DB="ldb_test_${TESTFILE_NAME}"
else
    # parallel tests all run in the same database
    TEST_CASE_DB="ldb_parallel"
fi

# Set database user
if [ -z "$DB_USER" ]
then
     echo "ERROR: DB_USER environment variable is not set before test_runner.sh is run by pg_regress"
     exit 1
fi

# Drop db after each test on exit signal
function drop_db {
  cat <<EOF | psql "$@" -U ${DB_USER} -d postgres -v ECHO=none >/dev/null 2>&1
    SET client_min_messages=ERROR;
    DROP DATABASE "${TEST_CASE_DB}";
EOF
}

# If these aren't parallel tests always drop the db after the test
# if they are though we only want to drop after end which is where we check invariants
# this allows the parallel tests to be run against the same db 
if [ "$PARALLEL" -eq 0 ]; then
    trap drop_db EXIT
elif [[ "$TESTFILE_NAME" =~ ^end ]]; then
    trap drop_db EXIT
fi


# Change directory to sql directory so sql imports will work correctly
cd sql/

# install lantern extension
# if tests are parallel we only do this for the begin tests as we won't be dropping the database until the end
# begin will handle initialization specific to the tests but expects the database already exists
if [ "$PARALLEL" -eq 0 ] || ( [[ "$TESTFILE_NAME" =~ ^begin ]] && [ "$PARALLEL" -eq 1 ] ); then
    psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "DROP DATABASE IF EXISTS ${TEST_CASE_DB};" 2>/dev/null
    psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "CREATE DATABASE ${TEST_CASE_DB};" 2>/dev/null
    psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -c "SET client_min_messages=error; CREATE EXTENSION lantern;" 2>/dev/null
    psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -f utils/common.sql 2>/dev/null
fi

# Exclude debug/inconsistent output from psql
# So tests will always have the same output
psql -U ${DB_USER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     "$@" -d ${TEST_CASE_DB} 2>&1 | \
          sed  -e 's! Memory: [0-9]\{1,\}kB!!' \
               -e 's! Memory Usage: [0-9]\{1,\}kB!!' \
               -e 's! Average  Peak Memory: [0-9]\{1,\}kB!!' \
               -e 's! time=[0-9]\+\.[0-9]\+\.\.[0-9]\+\.[0-9]\+!!' | \
          grep -v 'DEBUG:  rehashing catalog cache id' | \
          grep -Gv '^ Planning Time:' | \
          grep -Gv '^ Execution Time:' | \
          # Only print debug messages followed by LANTERN
          perl -nle'print if !m{DEBUG:(?!.*LANTERN)}'
