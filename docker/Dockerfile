ARG PG_VERSION=15

FROM postgres:$PG_VERSION-bookworm
ARG PG_VERSION

WORKDIR /tmp/lantern

COPY . .

RUN PG_VERSION=$PG_VERSION ./ci/scripts/build-docker.sh
