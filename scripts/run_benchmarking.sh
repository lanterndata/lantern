#!/bin/bash
set -e

# Environment variables for benchmarking
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
export NONE_DATABASE_URL=postgres://postgres:postgres@localhost:5432/none
export NEON_DATABASE_URL=postgres://postgres:postgres@localhost:5432/neon
export PGVECTOR_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pgvector
export LANTERN_DATABASE_URL=postgres://postgres:postgres@localhost:5432/lantern

# Benchmarking parameters
BASE_PARAMS="--extension lantern --dataset sift --N 10k"
INDEX_PARAMS="--m 4 --ef_construction 128 --ef 10"

# Set up benchmarking
BENCHMARK_DIR="benchmark"
if [ ! -d "$BENCHMARK_DIR" ]; then
    git clone -b @di/restructure https://github.com/lanterndata/benchmark "$BENCHMARK_DIR"
    cd "$BENCHMARK_DIR"
else
    cd "$BENCHMARK_DIR"
    git pull origin @di/restructure
fi
pip install -r requirements.txt --break-system-packages
python3 -m core.setup --datapath /tmp/benchmark_data $BASE_PARAMS

# Run benchmarking
echo python3 -m core.benchmark_select $BASE_PARAMS $INDEX_PARAMS --K 5
python3 -m core.benchmark_select $BASE_PARAMS $INDEX_PARAMS --K 5
python3 -m core.benchmark_insert $BASE_PARAMS $INDEX_PARAMS
python3 -m core.benchmark_create $BASE_PARAMS $INDEX_PARAMS

# Retrieve metrics of current code
new_recall=$(python3 -m core.retrieve_benchmark_result $BASE_PARAMS $INDEX_PARAMS --metric RECALL --K 5)

# TODO: Retrieve metrics to compare against
old_recall=0

# Print old vs. new metrics in tabular format
divider_length=70
divider_line=$(printf "%-${divider_length}s" " " | tr ' ' '-')
printf "%s\n" "$divider_line"
printf "| %-20s | %20s | %20s |\n" "Metric" "Old" "New"
printf "%s\n" "$divider_line"
printf "| %-20s | %20.2f | %20.2f |\n" "Recall" $old_recall $new_recall
printf "%s\n" "$divider_line"

# Print warnings and errors
ERROR="0"

recall_difference=$(echo "$new_recall - $old_recall" | bc -l)
if (( $(echo "$recall_difference < 0" | bc -l) )); then
    if (( $(echo "$recall_difference < -0.05" | bc -l) )); then
        printf "ERROR: Recall decreased by %.2f\n" $recall_difference
        ERROR="1"
    else
        printf "WARNING: Recall decreased by %.2f\n" $recall_difference
    fi
elif (( $(echo "$new_recall == 1.0" | bc -l) )); then
    echo "ERROR: Recall should not be 1.0"
fi


# Exit with appropriate exit code
if [ "$ERROR" -ne "0" ]; then
    exit 1
fi
exit 0