#!/bin/bash
set -e

# This sets $ARCH and $PLATFORM env variables
source "$(dirname "$0")/../../scripts/get_arch_and_platform.sh"

if [[ $PLATFORM == "mac" ]]; then
   BUILD_SCRIPT="build-mac.sh"
elif [[ $PLATFORM == "linux" ]]; then
   BUILD_SCRIPT="build-linux.sh"
else
   echo "Invalid target use one of [mac, linux]"
   exit 1
fi

function setup_environment() {
  export BRANCH=${BRANCH_NAME:-dev}
  export POSTGRES_USER=postgres
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
}

function clone_or_use_source() {
  if [ -z ${USE_SOURCE} ]; then
    # Clone from git
    cd /tmp
    git clone --recursive https://github.com/lanterndata/lantern.git -b $BRANCH
  else 
    # Use already checkouted code
    shopt -s dotglob
    rm -rf /tmp/lantern
    mkdir -p /tmp/lantern
    cp -r -P ./* /tmp/lantern/
  fi
}

function install_external_dependencies() {
  # Install pgvector
  pushd /tmp
    PGVECTOR_VERSION=0.5.0
    wget -O pgvector.tar.gz https://github.com/pgvector/pgvector/archive/refs/tags/v${PGVECTOR_VERSION}.tar.gz
    tar xzf pgvector.tar.gz
    pushd pgvector-${PGVECTOR_VERSION}
      make && make install
    popd
  popd
}

function build_and_install() {
  cd /tmp/lantern
  mkdir build
  cd build

  flags="-DUSEARCH_NO_MARCH_NATIVE=ON"
  
  # Treat warnings as errors in CI/CD
  flags+=" -DCMAKE_COMPILE_WARNING_AS_ERROR=ON"
  
  if [ -n "$ENABLE_COVERAGE" ]
  then
    flags="$flags -DCMAKE_C_COMPILER=/usr/bin/gcc -DCODECOVERAGE=ON -DBUILD_C_TESTS=ON"
  fi

  # Run cmake
  cmake $flags ..
  make install
}

# Source platform specific build script
source "$(dirname "$0")/${BUILD_SCRIPT}"

setup_environment
setup_locale_and_install_packages
setup_postgres
install_external_dependencies
clone_or_use_source
build_and_install
package_if_necessary
cleanup_environment
