#!/bin/bash

get_cmake_flags(){
 echo "-DMARCH_NATIVE=OFF"
}

export DEBIAN_FRONTEND=noninteractive

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

# Set Locale
apt update && apt-mark hold locales && \
# Install required packages for build
apt install -y --no-install-recommends build-essential cmake postgresql-server-dev-$PG_VERSION && \
# Build lantern
cd /tmp/lantern && mkdir build && cd build && \
# Run cmake
sh -c "cmake $(get_cmake_flags) .." && \
make install && \
# Remove dev files
rm -rf /tmp/lantern && \
apt-get remove -y build-essential postgresql-server-dev-$PG_VERSION cmake && \
apt-get autoremove -y && \
apt-mark unhold locales && \
rm -rf /var/lib/apt/lists/*
