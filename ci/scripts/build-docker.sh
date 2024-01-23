#!/bin/bash

get_cmake_flags(){
 echo "-DBUILD_FOR_DISTRIBUTING=YES -DMARCH_NATIVE=OFF -DCMAKE_C_COMPILER=clang  -DCMAKE_CXX_COMPILER=clang"
}

export DEBIAN_FRONTEND=noninteractive

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

# Set Locale
apt update && apt-mark hold locales && \
# Install required packages for build
apt install -y --no-install-recommends build-essential cmake clang llvm postgresql-server-dev-$PG_VERSION && \
# Build lantern
cd /tmp/lantern && mkdir build && cd build && \
# Run cmake
sh -c "cmake $(get_cmake_flags) .." && \
make install && \
# Remove dev files
rm -rf /tmp/lantern && \
apt-get remove -y build-essential postgresql-server-dev-$PG_VERSION cmake clang llvm && \
apt-get autoremove -y && \
apt-mark unhold locales && \
rm -rf /var/lib/apt/lists/*
