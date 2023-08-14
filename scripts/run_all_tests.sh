#!/bin/bash
IFS=$'\n\t'

TESTDB=testdb
PSQL=psql
TMP_ROOT=/tmp/lanterndb
TMP_OUTDIR=$TMP_ROOT/tmp_output
FILTER="${FILTER:-}"
DB_USER="${DB_USER:-}"
# this will be used by pg_regress while making diff file
export PG_REGRESS_DIFF_OPTS=-u

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


if [ -z $FILTER ]
then
    SCHEDULE=schedule.txt
else
    TEST_FILES=$(cat schedule.txt | sed -e 's/test://' | tr " " "\n" | sed -e '/^$/d')
    
    rm -rf $TMP_OUTDIR/schedule.txt
    while IFS= read -r f; do
        echo "Checking $f"
        if [[ $f == *"$FILTER"* ]]; then
            echo "test: $f" >> $TMP_OUTDIR/schedule.txt
        fi
    done <<< "$TEST_FILES"

    
    if [ ! -f "$TMP_OUTDIR/schedule.txt" ]
    then
        echo "No tests matches filter \"$FILTER\""
        exit 0
    fi
    
    SCHEDULE=$TMP_OUTDIR/schedule.txt
fi

function print_diff {
    if [ -f "$TMP_OUTDIR/regression.diffs" ]
    then
        cat $TMP_OUTDIR/regression.diffs
    fi
}

trap print_diff ERR

$(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --schedule=$SCHEDULE --outputdir=$TMP_OUTDIR --launcher=./test_runner.sh
