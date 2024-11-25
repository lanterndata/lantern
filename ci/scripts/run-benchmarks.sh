#!/bin/bash
set -e

source "$(dirname "$0")/utils.sh"

export WORKDIR=/tmp/lantern
export PG_VERSION=17
export GITHUB_OUTPUT=/dev/null
export PGDATA=/var/lib/postgresql/$PG_VERSION/main

echo "port = 5432" >> ${PGDATA}/postgresql.conf
echo "shared_preload_libraries = 'lantern_extras' " >> ${PGDATA}/postgresql.conf
# Enable auth without password
echo "local   all             all                                     trust" >  $PGDATA/pg_hba.conf
echo "host    all             all             127.0.0.1/32            trust" >>  $PGDATA/pg_hba.conf
echo "host    all             all             ::1/128                 trust" >>  $PGDATA/pg_hba.conf

POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres 1>/tmp/pg-out.log 2>/tmp/pg-error.log &
wait_for_pg
cd $WORKDIR/lantern_hnsw/build

export DATABASE_URL=postgresql://localhost:5432/postgres
export LANTERN_DATABASE_URL=postgresql://localhost:5432/postgres
git clone https://github.com/lanterndata/benchmark -b varik/fix-external-indexing
cd benchmark
pip install -r core/requirements.txt
pip install -r external/requirements.txt
cd ..

make benchmark-skip-setup
killall postgres

