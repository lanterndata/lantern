#!/bin/bash

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

if [ -z "$GITHUB_OUTPUT" ]
then
  export GITHUB_OUTPUT=/dev/null
fi

export PGDATA=/etc/postgresql/$PG_VERSION/main/
# Set port
echo "port = 5432" >> $PGDATA/postgresql.conf
# Enable auth without password
echo "local   all             all                                     trust" >  $PGDATA/pg_hba.conf
echo "host    all             all             127.0.0.1/32            trust" >>  $PGDATA/pg_hba.conf
echo "host    all             all             ::1/128                 trust" >>  $PGDATA/pg_hba.conf

apt install python3-pip git -y && \
cd /tmp && git clone https://github.com/lanterndata/benchmark.git -b narek-setup-script && \
cd /tmp/benchmark && sudo pip3 install -r requirements.txt && \
chown -R postgres:postgres /tmp/benchmark

su postgres <<EOSU
wait_for_pg(){
 tries=0
 until pg_isready -U postgres 2>/dev/null; do
   if [ $tries -eq 10 ];
   then
     echo "Can not connect to postgres"
     exit 1
   fi
   
   sleep 1
   tries=$((tries+1))
 done
}
# Run postgres database
POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres 1>/tmp/pg-out.log 2>/tmp/pg-error.log &
# Wait for start and run tests
wait_for_pg && \
export DATABASE_URL='postgresql://localhost:5432/testdb';
# Install dependencies
psql -c 'create database testdb;' && \
psql -c 'create extension if not exists vector;' testdb && \
psql -c 'create extension if not exists lanterndb;' testdb && \
cd /tmp/benchmark/experiments/scripts && python3 ./setup_tables.py && \
cd /tmp/benchmark/experiments && python3 ./recall_experiment.py --dataset sift --extension pgvector --N 10k 1m && \
psql -c 'SELECT * FROM experiment_results;' testdb && \
psql -c "COPY experiment_results TO '/tmp/benchmark.csv' DELIMITER ',' CSV HEADER;" testdb
EOSU
