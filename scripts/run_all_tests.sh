#!/bin/bash
# bash strict mode
set -euo pipefail
IFS=$'\n\t'

TESTDB=testdb
PSQL=psql
TMP_ROOT=/tmp/lanterndb
TMP_OUTDIR=$TMP_ROOT/tmp_output
FILTER="${FILTER:-}"
# read the first command line argument

echo "Filter: $FILTER"

mkdir -p $TMP_OUTDIR

# if $TMP_ROOT/vector_datasets does not exist
# create the folder
if [ ! -d "$TMP_ROOT/vector_datasets" ]
then
    mkdir -p $TMP_ROOT/vector_datasets
    pushd $TMP_ROOT/vector_datasets
        wget https://storage.googleapis.com/lanterndb/sift_base1k.csv
        wget https://storage.googleapis.com/lanterndb/tsv_wiki_sample.csv
        # Convert vector to arrays to be used with real[] type
        cat sift_base1k.csv | sed -e 's/\[/{/g' | sed -e 's/\]/}/g' > sift_base1k_arrays.csv
    popd
fi

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

    # psql options
    # -e: echo commands
    # -E: (passed manually, for debugging) echo hidden magic commands (\d, \di+, etc)
    ${PSQL} testdb --quiet -f test/sql/test_helpers/common.sql > /dev/null
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

