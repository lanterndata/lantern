#!/bin/bash
set -e

WORKDIR=/tmp/lantern
PG_VERSION=${PG_VERSION:-15}
export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"

source "$(dirname "$0")/utils.sh"

# Start database
brew services start postgresql@$PG_VERSION

wait_for_pg && cd $WORKDIR/lantern_hnsw/build && make test && make test-parallel
