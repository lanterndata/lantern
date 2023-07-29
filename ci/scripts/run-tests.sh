#!/bin/bash

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

export WORKDIR=/tmp/lanterndb

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

export PGDATA=/etc/postgresql/$PG_VERSION/main/
# Set port
echo "port = 5432" >> $PGDATA/postgresql.conf
# Run postgres database
POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres &>/dev/null &
# Wait for start and run tests
wait_for_pg && cd $WORKDIR/build && make test
