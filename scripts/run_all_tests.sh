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

# if $TMP_ROOT/files does not exist
# create the folder
if [ ! -d "$TMP_ROOT/files" ]
then
    mkdir -p $TMP_ROOT/files
    echo "Downloading necessary files for tests..."
    pushd $TMP_ROOT/files
       # Index file to test version compatibility
       curl -sSo index-sift1k-l2-0.0.0.usearch https://storage.googleapis.com/lanterndata/lanterndb_binary_indexes/index-sift1k-l2-v2.usearch
       # Actual index files
       curl -sSo index-sift1k-cos.usearch https://storage.googleapis.com/lanterndata/lanterndb_binary_indexes/index-sift1k-cos-v3.usearch
       curl -sSo index-sift1k-l2.usearch https://storage.googleapis.com/lanterndata/lanterndb_binary_indexes/index-sift1k-l2-v3.usearch
       # Corrupted index file for test
       tail -c +100 index-sift1k-l2.usearch > index-sift1k-l2-corrupted.usearch
    popd
    echo "Successfully downloaded all necessary test files"
fi

# Check if pgvector is available
pgvector_installed=$($PSQL -U $DB_USER -d postgres -c "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'" -tA)

# Generate schedule.txt
rm -rf $TMP_OUTDIR/schedule.txt
if [ -n "$FILTER" ]; then
    if [[ "$pgvector_installed" == "1" ]]; then
        TEST_FILES=$(cat schedule.txt | grep -E '^(test:|test_pgvector:)' | sed -e 's/^\(test:\|test_pgvector:\)//' | tr " " "\n" | sed -e '/^$/d')
    else
        TEST_FILES=$(cat schedule.txt | grep '^test:' | sed -e 's/^test://' | tr " " "\n" | sed -e '/^$/d')
    fi

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
else
    while IFS= read -r line; do
        if [[ "$line" =~ ^test_pgvector: ]]; then
            test_name=$(echo "$line" | sed -e 's/test_pgvector://')
            if [ "$pgvector_installed" == "1" ]; then
                echo "test: $test_name" >> $TMP_OUTDIR/schedule.txt
            fi
        else
            echo "$line" >> $TMP_OUTDIR/schedule.txt
        fi
    done < schedule.txt
fi
SCHEDULE=$TMP_OUTDIR/schedule.txt

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
