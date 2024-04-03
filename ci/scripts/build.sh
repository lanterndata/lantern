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
  export PGVECTOR_VERSION=0.6.1
  #fix pg_cron at the latest commit of the time
  export PG_CRON_COMMIT_SHA=7e91e72b1bebc5869bb900d9253cc9e92518b33f
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
    wget -O pgvector.tar.gz https://github.com/pgvector/pgvector/archive/refs/tags/v${PGVECTOR_VERSION}.tar.gz
    tar xzf pgvector.tar.gz
    rm -rf pgvector || true
    mv pgvector-${PGVECTOR_VERSION} pgvector
    pushd pgvector
      make -j && make install
    popd

  popd
}

function build_and_install() {
  cd /tmp/lantern
  mkdir build
  cd build

  flags="-DBUILD_FOR_DISTRIBUTING=YES -DMARCH_NATIVE=OFF -DCMAKE_COMPILE_WARNING_AS_ERROR=ON \
  -DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX \
  -DBUILD_C_TESTS=ON"

  if [[ "$ENABLE_COVERAGE" == "1" ]]
  then
    flags="$flags -DCODECOVERAGE=ON"
    mv /usr/bin/gcov-13 /usr/bin/gcov
  fi

  # Run cmake
  cmake $flags ..
  make install -j
}

setup_environment

# Source platform specific build script
source "$(dirname "$0")/${BUILD_SCRIPT}"

setup_locale_and_install_packages
setup_postgres
install_external_dependencies
install_platform_specific_dependencies
clone_or_use_source
build_and_install
package_if_necessary
cleanup_environment
