#!/bin/bash
set -e

function setup_environment() {
  export BRANCH=${BRANCH_NAME:-dev}
  export POSTGRES_USER=postgres
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
  export PGVECTOR_VERSION=0.7.4-lanterncloud
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
  if [[ $PG_VERSION -gt 12 ]]; then
    # Install pgvector
    pushd /tmp
      rm -rf pgvector || true
      git clone --recursive https://github.com/lanterndata/pgvector.git -b "v${PGVECTOR_VERSION}"
      pushd pgvector
        # Set max ef_search to 50000
        # .bak trick is needed to make this work on both mac and linux
        # https://stackoverflow.com/questions/5694228/sed-in-place-flag-that-works-both-on-mac-bsd-and-linux
        sed -i.bak "s/#define HNSW_MAX_EF_SEARCH.*/#define HNSW_MAX_EF_SEARCH 50000/g" src/hnsw.h
        OLDCC=$CC
        OLDCXX=$CXX
        export CC=/usr/bin/clang-15
        export CXX=/usr/bin/clang++-15
        make -j && make install
        export CC=$OLDCC
        export CXX=$OLDCXX
      popd

    popd
  fi
}

function build_and_install_lantern() {
  rm -rf /tmp/lantern/lantern_hnsw/build || true 2>/dev/null
  pushd /tmp/lantern/lantern_hnsw
    # install ruby dependencies for test_updates
    pushd /tmp/lantern/lantern_hnsw/scripts/test_updates
      bundler
    popd

    # install update and WAL test dependencies
    python3 -m venv cienv
    source cienv/bin/activate
    pip install -r /tmp/lantern/lantern_hnsw/scripts/requirements.txt

    flags="-DBUILD_FOR_DISTRIBUTING=YES -DMARCH_NATIVE=OFF -DCMAKE_COMPILE_WARNING_AS_ERROR=ON \
    -DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX -DUSE_SSL=$USE_SSL"

    if [[ "$ENABLE_COVERAGE" == "1" ]]
    then
      flags="$flags -DCODECOVERAGE=ON"
      cp /usr/bin/gcov-12 /usr/bin/gcov
    fi
    
    if [[ "$ENABLE_FAILURE_POINTS" == "1" ]]
    then
      flags="$flags -DFAILURE_POINTS=ON"
    fi

    # Run cmake
    cmake $flags -B build
    make -C build install -j
  popd
}

function wait_for_pg(){
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
