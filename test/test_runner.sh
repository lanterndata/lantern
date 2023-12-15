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

function run_regression_test {
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
}



# Change directory to sql so sql imports will work correctly
cd sql/

# install lantern extension
if [[ "$PARALLEL" -eq 0 || "$TESTFILE_NAME" == "begin" ]]; then
     psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "DROP DATABASE IF EXISTS ${TEST_CASE_DB};" 2>/dev/null
     psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "CREATE DATABASE ${TEST_CASE_DB};" 2>/dev/null
fi
if [ ! -z "$UPDATE_EXTENSION" ]
then
     if [ -z "$UPDATE_FROM" ] || [ -z "$UPDATE_TO" ]
     then
          echo "ERROR: UPDATE_FROM and UPDATE_TO environment variables must be set before test_runner.sh whenever UPDATE_EXTENSION is set"
          exit 1
     fi

     # print all available migrations with the line below:
     # psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -q -c "SELECT * FROM pg_extension_update_paths('lantern');" 2>/dev/null
     # install the old version of the extension and sanity-check that all tests pass
     psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -c "SET client_min_messages=error; CREATE EXTENSION IF NOT EXISTS lantern VERSION '$UPDATE_FROM';"
     # upgrade to the new version of the extension and make sure that all existing tests still pass
     # todo:: this approach currently is broken for pgvector-compat related upgrade scripts as that regression test drops
     # and recreates the extension so whatever we do here is ignored
     # parallel tests run into issues when multiple instances of the runner simultaneously reindex, we need to track when
     # this occurs and make the process conditional on it
     LOCKFILE="/tmp/ldb_update.lock"
     FINISHEDFILE="/tmp/ldb_update_finished"

     (
         if flock -xn 200; then
             if [ ! -f "$FINISHEDFILE" ]; then
                 psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -f utils/common.sql 2>/dev/null
                 psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -c "SET client_min_messages=error; ALTER EXTENSION lantern UPDATE TO '$UPDATE_TO';"
                 touch $FINISHEDFILE
             fi
         fi
     ) 200>"$LOCKFILE"

     while [ ! -f "$FINISHEDFILE" ]; do
         sleep 1
     done

     run_regression_test $@
else

     psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -c "SET client_min_messages=error; CREATE EXTENSION lantern;" 2>/dev/null
     psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -f utils/common.sql 2>/dev/null

     run_regression_test $@
fi
