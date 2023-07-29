#!/bin/bash

get_cmake_flags(){
 # TODO:: remove after test
 echo "-DUSEARCH_NO_MARCH_NATIVE=ON"
 # if [[ $ARCH == *"arm"* ]]; then
 #   echo "-DUSEARCH_NO_MARCH_NATIVE=ON"
 # fi
}

export BRANCH=$BRANCH_NAME
export POSTGRES_USER=postgres
export DEBIAN_FRONTEND=noninteractive

if [ -z "$BRANCH" ]
then
  BRANCH="dev"
fi

if [ -z "$PG_VERSION" ]
then
  export PG_VERSION=15
fi

if [ -z "$GITHUB_OUTPUT" ]
then
  export GITHUB_OUTPUT=/dev/null
fi

# Set Locale
echo "LC_ALL=en_US.UTF-8" > /etc/environment && \
echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && \
echo "LANG=en_US.UTF-8" > /etc/locale.conf && \
apt update -y && apt install locales -y && \
locale-gen en_US.UTF-8 && \
# Install required packages for build
apt install lsb-core build-essential automake cmake wget git dpkg-dev wget -y && \
# Add postgresql apt repo
export ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH) && \
sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add - &&\
# Install postgres and dev files for C headers
apt update && apt install postgresql-$PG_VERSION postgresql-server-dev-$PG_VERSION -y
# Install pgvector
apt install postgresql-$PG_VERSION-pgvector -y
# Fix pg_config (sometimes it points to wrong version)
rm -f /usr/bin/pg_config && ln -s /usr/lib/postgresql/$PG_VERSION/bin/pg_config /usr/bin/pg_config

if [ -z ${USE_SOURCE+x} ]; then
  # Clone from git
  cd /tmp && git clone --recursive https://github.com/lanterndata/lanterndb.git -b $BRANCH
else 
  # Use already checkouted code
  mkdir -p /tmp/lanterndb && cp -r ./* /tmp/lanterndb/
fi

cd /tmp/lanterndb && mkdir build && cd build && \
# Run cmake
sh -c "cmake $(get_cmake_flags) .." && \
make install && \
# Bundle debian packages && \
cpack &&
 
# Print package name to github output
export EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}') && \
export PACKAGE_NAME=lanterndb-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}.deb && \
echo "package_name=$PACKAGE_NAME" >> "$GITHUB_OUTPUT" && \
echo "package_path=$(pwd)/$(ls *.deb | tr -d '\n')" >> "$GITHUB_OUTPUT"

# Chown to postgres for running tests
chown -R postgres:postgres /tmp/lanterndb
