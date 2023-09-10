#!/bin/bash
set -e

# Benchmarking parameters
BASE_PARAMS="--extension lantern --dataset sift --N 10k"
INDEX_PARAMS="--m 4 --ef_construction 128 --ef 10"
PARAMS="$BASE_PARAMS $INDEX_PARAMS --K 5"

# Settings
SKIP_SETUP=0
PRINT_ONLY=0
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-setup) SKIP_SETUP=1 ;;
        --print-only) PRINT_ONLY=1 ;;
    esac
    shift
done

# Go to benchmark directory
cd benchmark

# Run setup
if [ "$SKIP_SETUP" -ne 1 ] && [ "$PRINT_ONLY" -ne 1 ]; then
    echo "Running data setup"
    python3 -m core.setup --datapath /tmp/benchmark_data $BASE_PARAMS
else
    echo "Skipping data setup"
fi

# Run benchmarks
if [ "$PRINT_ONLY" -ne 1 ]; then
    echo "Running benchmarks"
    python3 -m external.run_benchmarks $PARAMS
fi

# Render benchmarks
python3 -m external.show_benchmarks $PARAMS --loginfo
python3 -m external.show_benchmarks $PARAMS --markdown > /tmp/benchmarks-out.md
python3 -m external.validate_benchmarks $PARAMS
python3 -m external.get_benchmarks_json $PARAMS > /tmp/benchmarks-out.json
