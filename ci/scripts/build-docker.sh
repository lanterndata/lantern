#!/bin/bash

get_cmake_flags(){
 # TODO:: remove after test
 echo "-DUSEARCH_NO_MARCH_NATIVE=ON"
 # if [[ $ARCH == *"arm"* ]]; then
 #   echo "-DUSEARCH_NO_MARCH_NATIVE=ON"
 # fi
}

export DEBIAN_FRONTEND=noninteractive

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

# Set Locale
apt update && apt-mark hold locales && \
# Install required packages for build
apt install -y --no-install-recommends build-essential cmake postgresql-server-dev-$PG_VERSION postgresql-$PG_VERSION-pgvector && \
# Build lanterndb
cd /tmp/lanterndb && mkdir build && cd build && \
# Run cmake
sh -c "cmake $(get_cmake_flags) .." && \
make install && \
# Remove dev files
rm -rf /tmp/lanterndb && \
apt-get remove -y build-essential postgresql-server-dev-$PG_VERSION cmake && \
apt-get autoremove -y && \
apt-mark unhold locales && \
rm -rf /var/lib/apt/lists/*
