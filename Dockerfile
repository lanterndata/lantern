ARG PG_VERSION=15
FROM postgres:$PG_VERSION
ARG PG_VERSION

COPY . /tmp/lanterndb

RUN PG_VERSION=$PG_VERSION ./tmp/lanterndb/ci/scripts/build-docker.sh 
