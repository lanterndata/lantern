#!/bin/bash
IFS=$'\n\t'

TESTDB=testdb
PSQL=psql
TMP_ROOT=/tmp/lantern
TMP_OUTDIR=$TMP_ROOT/tmp_output
FILTER="${FILTER:-}"
EXCLUDE="${EXCLUDE:-}"
# $USER is not set in docker containers, so use whoami
DEFAULT_USER=$(whoami)

if [[ -n "$FILTER" && -n "$EXCLUDE" ]]; then
    echo "-FILTER and -EXCLUDE cannot be used together, please use only one"
    exit 1
fi

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
pgvector_installed=$($PSQL -U $DB_USER -d postgres -c "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'" -tA | tail -n 1 | tr -d '\n')

# Settings
REGRESSION=0
PARALLEL=0
C_TESTS=0
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --regression) REGRESSION=1 ;;
        --parallel) PARALLEL=1 ;;
        --c) C_TESTS=1 ;;
    esac
    shift
done

if [ "$C_TESTS" -eq 1 ]; then
    DB_USER=$DB_USER TEST_DB_NAME=$TESTDB ./bin/lantern_c_tests
    exit $?
fi

FIRST_TEST=1
function print_test {
    if [ "$PARALLEL" -eq 1 ]; then
        if [ $1 == end ]; then
            echo -e "\ntest: $1" >> $2
        elif [ $1 == begin ]; then
            echo -e "\ntest: $1" >> $2
        else
            if [ "$FIRST_TEST" -eq 1 ]; then
                echo -n "test: $1" >> $2
                FIRST_TEST=0
            else
                echo -n " $1" >> $2
            fi
        fi
    else
        echo "test: $1" >> $2
    fi
}

# Generate schedule.txt
rm -rf $TMP_OUTDIR/schedule.txt
if [ "$PARALLEL" -eq 1 ]; then
    SCHEDULE='parallel_schedule.txt'
else
    SCHEDULE='schedule.txt'
fi
if [[ -n "$FILTER" || -n "$EXCLUDE" ]]; then
    if [ "$PARALLEL" -eq 1 ]; then
    	TEST_FILES=$(cat $SCHEDULE | grep -E '^(test:|test_begin:|test_end:)' | sed -E -e 's/^test:|test_begin:|test_end://' | tr " " "\n" | sed -e '/^$/d')
    else
        if [[ "$pgvector_installed" == "1" ]]; then
            TEST_FILES=$(cat $SCHEDULE | grep -E '^(test:|test_pgvector:)' | sed -E -e 's/^test:|test_pgvector://' | tr " " "\n" | sed -e '/^$/d')
        else
            TEST_FILES=$(cat $SCHEDULE | grep '^test:' | sed -e 's/^test://' | tr " " "\n" | sed -e '/^$/d')
        fi
    fi

    while IFS= read -r f; do
        if [ -n "$FILTER" ]; then
            if [[ $f == *"$FILTER"* ]]; then
                print_test $f $TMP_OUTDIR/schedule.txt $FIRST_TEST
            fi
        elif [ -n "$EXCLUDE" ]; then
            if [[ $f == *"$EXCLUDE"* ]]; then
                continue
            fi
            print_test $f $TMP_OUTDIR/schedule.txt $FIRST_TEST
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
        elif [[ "$line" =~ ^test_begin: ]]; then
            test_name=$(echo "$line" | sed -e 's/test_begin:/test:/')
            echo "$test_name" >> $TMP_OUTDIR/schedule.txt
        elif [[ "$line" =~ ^test_end: ]]; then
            test_name=$(echo "$line" | sed -e 's/test_end:/test:/')
            echo "$test_name" >> $TMP_OUTDIR/schedule.txt
        else
            echo "$line" >> $TMP_OUTDIR/schedule.txt
        fi
    done < $SCHEDULE
fi
unset $SCHEDULE
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

if [ "$PARALLEL" -eq 1 ]; then
    cd parallel
    PARALLEL=$PARALLEL DB_USER=$DB_USER $(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --user=$DB_USER --schedule=$SCHEDULE --outputdir=$TMP_OUTDIR --launcher=../test_runner.sh
else
    PARALLEL=$PARALLEL DB_USER=$DB_USER $(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress --user=$DB_USER --schedule=$SCHEDULE --outputdir=$TMP_OUTDIR --launcher=./test_runner.sh
fi
