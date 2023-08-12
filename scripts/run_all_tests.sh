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

if [ -z "$FILTER" ] 
then
 $(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --schedule=schedule.txt --outputdir=$TMP_OUTDIR --dbname=$TESTDB
else
 $(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --outputdir=$TMP_OUTDIR $FILTER --dbname=$TESTDB
fi

 cat $TMP_OUTDIR/regression.diffs 2>/dev/null
