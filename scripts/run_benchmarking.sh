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

# Allow skipping setup
SKIP_SETUP=0

# Pull benchmarking repo
BENCHMARK_DIR="benchmark"
if [ ! -d "$BENCHMARK_DIR" ]; then
    git clone https://github.com/lanterndata/benchmark "$BENCHMARK_DIR"
    cd "$BENCHMARK_DIR"
else
    cd "$BENCHMARK_DIR"

    # Optionally pull only when needed
    read -p "Do you want to pull the latest changes? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf benchmark
        git pull origin main
    else
        SKIP_SETUP=1
    fi
fi

# Optionally install requirements only when needed
if [ $SKIP_SETUP -eq 0 ]; then
    pip install -r requirements.txt --break-system-packages
fi

# Optionally run setup only when needed
if [ $SKIP_SETUP -eq 0 ] || [ ! -d "/tmp/benchmark_data" ]; then
    python3 -m core.setup --datapath /tmp/benchmark_data $BASE_PARAMS
else
    echo "Skipping data setup as it seems to have been done recently."
fi

# Check if the user wants to just view the recent benchmarking results.
read -p "Print recent results only (no re-run)? [y/N]" -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Print the most recent benchmarking results.
    python3 -m external.run_benchmarking $BASE_PARAMS $INDEX_PARAMS --K 5 --print-only
else
    # Execute the benchmarking process and then display the results.
    python3 -m external.run_benchmarking $BASE_PARAMS $INDEX_PARAMS --K 5
fi