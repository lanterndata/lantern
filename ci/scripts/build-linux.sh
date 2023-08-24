#!/bin/bash
set -e

function setup_environment() {
  export BRANCH=${BRANCH_NAME:-dev}
  export POSTGRES_USER=postgres
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
}

function setup_locale_and_install_packages() {
  echo "LC_ALL=en_US.UTF-8" > /etc/environment
  echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
  echo "LANG=en_US.UTF-8" > /etc/locale.conf

  apt update -y
  apt install -y locales lsb-core build-essential automake cmake wget git dpkg-dev gcovr clang-format

  locale-gen en_US.UTF-8
}

function setup_postgres() {
  # Add postgresql apt repo
  export ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
  echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add -
  # Install postgres and dev files for C headers
  apt update
  apt install -y postgresql-$PG_VERSION postgresql-server-dev-$PG_VERSION
  # Install pgvector
  apt install -y postgresql-$PG_VERSION-pgvector
  # Fix pg_config (sometimes it points to wrong version)
  rm -f /usr/bin/pg_config && ln -s /usr/lib/postgresql/$PG_VERSION/bin/pg_config /usr/bin/pg_config
}

function clone_or_use_source() {
  if [ -z ${USE_SOURCE} ]; then
    # Clone from git
    cd /tmp
    git clone --recursive https://github.com/lanterndata/lanterndb.git -b $BRANCH
  else 
    # Use already checkouted code
    shopt -s dotglob
    mkdir -p /tmp/lanterndb
    cp -r ./* /tmp/lanterndb/
  fi
}

function build_and_install() {
  cd /tmp/lanterndb
  mkdir build
  cd build

   # TODO:: remove after test
  flags="-DUSEARCH_NO_MARCH_NATIVE=ON"
  # if [[ $ARCH == *"arm"* ]]; then
  #   echo "-DUSEARCH_NO_MARCH_NATIVE=ON"
  # fi
  if [ -n "$ENABLE_COVERAGE" ]
  then
    flags="$flags -DCMAKE_C_COMPILER=/usr/bin/gcc -DCODECOVERAGE=ON"
  fi

  # Run cmake
  cmake $flags ..
  make install
}

function package_if_necessary() {
  if [ -n "$BUILD_PACKAGES" ]; then
    # Bundle debian packages
    cpack &&
    # Print package name to github output
    export EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}') && \
    export PACKAGE_NAME=lanterndb-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}.deb && \

    echo "package_version=$EXT_VERSION" >> "$GITHUB_OUTPUT" && \
    echo "package_name=$PACKAGE_NAME" >> "$GITHUB_OUTPUT" && \
    echo "package_path=$(pwd)/$(ls *.deb | tr -d '\n')" >> "$GITHUB_OUTPUT"
  fi
}

setup_environment
setup_locale_and_install_packages
setup_postgres
clone_or_use_source
build_and_install
package_if_necessary

# Chown to postgres for running tests
chown -R postgres:postgres /tmp/lanterndb