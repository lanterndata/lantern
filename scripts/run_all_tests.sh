#!/bin/bash
# bash strict mode
set -euo pipefail
IFS=$'\n\t'

TESTDB=testdb
PSQL=psql
TMP_OUTDIR=/tmp/lanterndb/tmp_output
FILTER="${FILTER:-}"
# read the first command line argument

echo "Filter: $FILTER"

mkdir -p $TMP_OUTDIR

for testfile in test/sql/*
do
    # if FILTER is set and testfile does not contain FILTER, skip
    if [ ! -z "$FILTER" ] && [[ ! $testfile =~ $FILTER ]]
    then
        continue
    fi
    echo "------------------------------------------------------------------------------------------------------"
    echo "-------------------------------- TESTING $testfile -------------------------------------------"
    echo "------------------------------------------------------------------------------------------------------"
    ${PSQL} postgres  -c "drop database if exists ${TESTDB};"
    ${PSQL} postgres  -c "create database ${TESTDB};"
    base=$(basename $testfile .sql)
    ${PSQL} testdb -ef test/sql/$base.sql > $TMP_OUTDIR/$base.out 2>&1 || true
    DIFF=$(diff test/expected/$base.out $TMP_OUTDIR/$base.out || true)
    # diff has non-zero exit code if files differ. ||true gets rid of error value
    # we can use the actual $DIFF in files to know whether the test failed
    # this avoids early script failure (see bash strict mode pipefail)
    if [ "$DIFF" != "" ]
    then
        diff -y -W 250 test/expected/$base.out $TMP_OUTDIR/$base.out
        echo "Test failed!"
        exit 1
    fi
done

# DIFF=$(diff test/expected/debug_helpers.out $TMP_OUTDIR/debug_helpers.out || true)
# if [ "$DIFF" != "" ]
# then
#     diff -y -W 250 test/expected/debug_helpers.out $TMP_OUTDIR/debug_helpers.out
#     echo "Test failed!"
#     exit 1
# fi

echo "Success!"

