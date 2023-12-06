#!/bin/bash
set -e

WORKDIR=/tmp/lantern
PG_VERSION=${PG_VERSION:-15}
export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"

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

# Start database
brew services start postgresql@$PG_VERSION

wait_for_pg && cd $WORKDIR/build && make test && make test-parallel && make test-client
