#!/bin/bash
set -e

WORKDIR=/tmp/lantern
GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
PG_VERSION=${PG_VERSION:-/dev/null}

export PGDATA=/etc/postgresql/$PG_VERSION/main/

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

# Set port
echo "port = 5432" >> ${PGDATA}postgresql.conf
# Run postgres database
GCOV_PREFIX=$WORKDIR/build/CMakeFiles/lantern.dir/ GCOV_PREFIX_STRIP=5 POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres 1>/tmp/pg-out.log 2>/tmp/pg-error.log &
# Wait for start and run tests
wait_for_pg && cd $WORKDIR/build && make test && \
killall postgres && \
gcovr -r $WORKDIR/src/ --object-directory $WORKDIR/build/ --xml /tmp/coverage.xml
