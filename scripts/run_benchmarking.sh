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
    git clone https://github.com/lanterndata/benchmark "$BENCHMARK_DIR"
    cd "$BENCHMARK_DIR"
else
    cd "$BENCHMARK_DIR"
    git pull origin main
fi
pip install -r requirements.txt --break-system-packages
python3 -m core.setup --datapath /tmp/benchmark_data $BASE_PARAMS

# Run benchmarking
python3 -m external.run_benchmarking $BASE_PARAMS $INDEX_PARAMS --K 5