#!/bin/bash
set -e

WORKDIR=/tmp/lantern
GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
PG_VERSION=${PG_VERSION:-15}
RUN_TESTS=${RUN_TESTS:-1}

export PGDATA=/etc/postgresql/$PG_VERSION/main

function wait_for_pg(){
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

function run_db_tests(){
  if [[ "$RUN_TESTS" == "1" ]]
  then
    cd $WORKDIR/build && \
    make test && \
    make test-c
    killall postgres && \
    gcovr -r $WORKDIR/src/ --object-directory $WORKDIR/build/ --xml /tmp/coverage.xml
  fi
}

function start_pg() {
  pg_response=$(pg_isready -U postgres 2>&1)

  if [[ $pg_response == *"accepting"* ]]; then
    echo "Postgres already running"
  elif [[ $pg_response == *"rejecting"* ]]; then
    echo "Postgres process is being killed retrying..."
    sleep 1
    start_pg
  else
    echo "port = 5432" >> ${PGDATA}/postgresql.conf
    # Enable auth without password
    echo "local   all             all                                     trust" >  $PGDATA/pg_hba.conf
    echo "host    all             all             127.0.0.1/32            trust" >>  $PGDATA/pg_hba.conf
    echo "host    all             all             ::1/128                 trust" >>  $PGDATA/pg_hba.conf


    # Set port
    echo "port = 5432" >> ${PGDATA}/postgresql.conf
    # Run postgres database
    GCOV_PREFIX=$WORKDIR/build/CMakeFiles/lantern.dir/ GCOV_PREFIX_STRIP=5 POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres 1>/tmp/pg-out.log 2>/tmp/pg-error.log &
  fi
}
# Wait for start and run tests
start_pg && wait_for_pg && run_db_tests
