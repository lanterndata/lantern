#!/bin/bash
IFS=$'\n\t'

TESTDB=testdb
PSQL=psql
TMP_ROOT=/tmp/lanterndb
TMP_OUTDIR=$TMP_ROOT/tmp_output
FILTER="${FILTER:-}"
# $USER is not set in docker containers, so use whoami
DEFAULT_USER=$(whoami)

# typically default user is root in a docker container
# and in those cases postgres is the user with appropriate permissions
# to the database
if [ "$DEFAULT_USER" == "root" ]
then
    DEFAULT_USER="postgres"
fi

DB_USER="${DB_USER:-$DEFAULT_USER}"
# this will be used by pg_regress while making diff file
export PG_REGRESS_DIFF_OPTS=-u

echo "Filter: $FILTER"

mkdir -p $TMP_OUTDIR

# if $TMP_ROOT/vector_datasets does not exist
# create the folder
if [ ! -d "$TMP_ROOT/vector_datasets" ]
then
    if ! command -v curl &> /dev/null; then
	echo "ERROR: The binary curl is required for running tests to download necessary vector test files"
	exit 1
    fi
    mkdir -p $TMP_ROOT/vector_datasets
    echo "Downloading necessary vector files..."
    pushd $TMP_ROOT/vector_datasets
        curl -sSo sift_base1k.csv https://storage.googleapis.com/lanterndb/sift_base1k.csv
        curl -sSo siftsmall_base.csv https://storage.googleapis.com/lanterndata/siftsmall/siftsmall_base.csv
        curl -sSo tsv_wiki_sample.csv https://storage.googleapis.com/lanterndb/tsv_wiki_sample.csv
        # Convert vector to arrays to be used with real[] type
        cat sift_base1k.csv | sed -e 's/\[/{/g' | sed -e 's/\]/}/g' > sift_base1k_arrays.csv
        cat siftsmall_base.csv | sed -e 's/\[/{/g' | sed -e 's/\]/}/g' > siftsmall_base_arrays.csv
    popd
    echo "Successfully Downloaded all necessary vector test files"
fi


if [ -z $FILTER ]
then
    SCHEDULE=schedule.txt
else
    TEST_FILES=$(cat schedule.txt | sed -e 's/test://' | tr " " "\n" | sed -e '/^$/d')

    rm -rf $TMP_OUTDIR/schedule.txt
    while IFS= read -r f; do
        if [[ $f == *"$FILTER"* ]]; then
            echo "test: $f" >> $TMP_OUTDIR/schedule.txt
        fi
    done <<< "$TEST_FILES"


    if [ ! -f "$TMP_OUTDIR/schedule.txt" ]
    then
        echo "NOTE: No tests matches filter \"$FILTER\""
        exit 0
    fi

    SCHEDULE=$TMP_OUTDIR/schedule.txt
fi

function print_diff {
    if [ -f "$TMP_OUTDIR/regression.diffs" ]
    then
        cat $TMP_OUTDIR/regression.diffs

        echo
        echo "Per-failed-test diff commands:"
        cat $TMP_OUTDIR/regression.diffs | grep -e '^diff -u .*expected/.*\.out .*/results/.*\.out$'
        echo
    fi
}

trap print_diff ERR

DB_USER=$DB_USER $(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --user=$DB_USER --schedule=$SCHEDULE --outputdir=$TMP_OUTDIR --launcher=./test_runner.sh
