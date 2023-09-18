#!/bin/bash
set -e

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

export WORKDIR=/tmp/lantern
export PG_VERSION=15
export GITHUB_OUTPUT=/dev/null
export PGDATA=/etc/postgresql/$PG_VERSION/main/

echo "port = 5432" >> $PGDATA/postgresql.conf
# Enable auth without password
echo "local   all             all                                     trust" >  $PGDATA/pg_hba.conf
echo "host    all             all             127.0.0.1/32            trust" >>  $PGDATA/pg_hba.conf
echo "host    all             all             ::1/128                 trust" >>  $PGDATA/pg_hba.conf

POSTGRES_HOST_AUTH_METHOD=trust /usr/lib/postgresql/$PG_VERSION/bin/postgres 1>/tmp/pg-out.log 2>/tmp/pg-error.log &
wait_for_pg
cd $WORKDIR/build

export LANTERN_DATABASE_URL=postgresql://localhost:5432/postgres
git clone https://github.com/lanterndata/benchmark
cd benchmark
pip install -r core/requirements.txt
pip install -r external/requirements.txt
cd ..

make benchmark
killall postgres

