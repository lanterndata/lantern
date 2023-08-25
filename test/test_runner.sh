#!/usr/bin/env bash

# Get current test file name
TESTFILE_NAME=${PGAPPNAME##pg_regress/}
# Set different name for each test database
# As pg_regress does not support cleaning db after each test
TEST_CASE_DB="ldb_test_${TESTFILE_NAME}"
# Set database user
if [ -z $DB_USER ]
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

trap drop_db EXIT


# Change directory to sql so sql imports will work correctly
cd sql/
# install lanterndb extension
psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "DROP DATABASE IF EXISTS ${TEST_CASE_DB};" 2>/dev/null
psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "DROP DATABASE IF EXISTS ${TEST_CASE_DB};" 2>/dev/null
psql "$@" -U ${DB_USER} -d postgres -v ECHO=none -q -c "CREATE DATABASE ${TEST_CASE_DB};" 2>/dev/null
psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -c "SET client_min_messages=error; CREATE EXTENSION vector; CREATE EXTENSION lanterndb;" 2>/dev/null
psql "$@" -U ${DB_USER} -d ${TEST_CASE_DB} -v ECHO=none -q -f utils/common.sql 2>/dev/null

# Exclude debug/inconsistent output from psql
# So tests will always have the same output
psql -U ${DB_USER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     "$@" -d ${TEST_CASE_DB} 2>&1 | \
          sed  -e 's! Memory: [0-9]\{1,\}kB!!' \
               -e 's! Memory Usage: [0-9]\{1,\}kB!!' \
               -e 's! Average  Peak Memory: [0-9]\{1,\}kB!!' | \
          grep -v 'DEBUG:  rehashing catalog cache id'
