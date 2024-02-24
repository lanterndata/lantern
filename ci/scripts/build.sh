#!/bin/bash

set -e

function setup_environment() {
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
}

function setup_onnx() {
  pushd /tmp
    ONNX_VERSION="1.16.1"
    PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ONNX_VERSION}/onnxruntime-linux-x64-${ONNX_VERSION}.tgz" && \
    if [[ $ARCH == *"arm"* ]]; then PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ONNX_VERSION}/onnxruntime-linux-aarch64-${ONNX_VERSION}.tgz"; fi && \
    mkdir -p /usr/local/lib && \
    cd /usr/local/lib && \
    wget $PACKAGE_URL && \
    tar xzf ./onnx*.tgz && \
    rm -rf ./onnx*.tgz && \
    mv ./onnx* ./onnxruntime && \
    echo /usr/local/lib/onnxruntime/lib > /etc/ld.so.conf.d/onnx.conf && \
    ldconfig
  popd
}

function setup_locale_and_install_packages() {
  echo "LC_ALL=en_US.UTF-8" > /etc/environment
  echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
  echo "LANG=en_US.UTF-8" > /etc/locale.conf

  apt update -y
  apt install -y --no-install-recommends lsb-release wget build-essential ca-certificates zlib1g-dev pkg-config libreadline-dev clang curl gnupg libssl-dev jq

  locale-gen en_US.UTF-8
  export ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
}

function setup_postgres() {
  # Add postgresql apt repo
  echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add -
  # Install postgres and dev files for C headers
  apt update
  apt install -y postgresql-$PG_VERSION postgresql-server-dev-$PG_VERSION
  # Fix pg_config (sometimes it points to wrong version)
  rm -f /usr/bin/pg_config && ln -s /usr/lib/postgresql/$PG_VERSION/bin/pg_config /usr/bin/pg_config
}

function setup_lantern() {
   LANTERN_VERSION=v0.2.0
    git clone --recursive https://github.com/lanterndata/lantern.git /tmp/lantern 
    pushd /tmp/lantern
      git checkout ${LANTERN_VERSION} && \
      git submodule update --recursive && \
      mkdir build 
      pushd build
        cmake -DMARCH_NATIVE=OFF -DBUILD_FOR_DSTRIBUTING=1 .. && \
        make install
      popd
    popd
}

function setup_rust() {
  curl -k -o /tmp/rustup.sh https://sh.rustup.rs
  chmod +x /tmp/rustup.sh
  /tmp/rustup.sh -y
  . "$HOME/.cargo/env"
}

function setup_cargo_deps() {
  if [ ! -d .cargo ]; then
  	mkdir .cargo
  fi
  echo "[target.$(rustc -vV | sed -n 's|host: ||p')]" >> .cargo/config
  cargo install cargo-pgrx --version 0.11.3
  cargo pgrx init "--pg$PG_VERSION" /usr/bin/pg_config
}

function package_cli() {
  source "$(dirname "$0")/get_arch_and_platform.sh"
  VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lantern_cli") | .version' | tr -d '"')
  PACKAGE_NAME=lantern-cli-${VERSION}-${PLATFORM}-${ARCH}
  SOURCE_DIR=$(pwd)
  BINARY_NAME=lantern-cli
  OUT_DIR=/tmp/${BINARY_NAME}
  BUILD_DIR=${SOURCE_DIR}/target/release/
  
  cargo build --package lantern_cli --release 

  mkdir -p ${OUT_DIR}
  
  cp ${BUILD_DIR}/${BINARY_NAME} $OUT_DIR
  
  pushd "$OUT_DIR"
    tar cf ${PACKAGE_NAME}.tar $BINARY_NAME
    ## Write output so we can use this in actions and upload artifacts
    echo "cli_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
    echo "cli_package_name=${PACKAGE_NAME}" >> $GITHUB_OUTPUT
  popd
}

function package_extension() {
  cargo pgrx package --pg-config /usr/bin/pg_config --package lantern_extras
  source "$(dirname "$0")/get_arch_and_platform.sh"

  EXT_VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lantern_extras") | .version' | tr -d '"')
  PACKAGE_NAME=lantern-extras-${EXT_VERSION}-postgres-${PG_VERSION}-${PLATFORM}-${ARCH}

  SOURCE_DIR=$(pwd)
  LIB_BUILD_DIR="$(pwd)/target/release/lantern_extras-pg${PG_VERSION}/usr/lib/postgresql/${PG_VERSION}/lib"
  SHARE_BUILD_DIR="$(pwd)/target/release/lantern_extras-pg${PG_VERSION}/usr/share/postgresql/${PG_VERSION}/extension"
  OUT_DIR=/tmp/lantern-extras
  
  mkdir -p ${OUT_DIR}/${PACKAGE_NAME}/lib

  # For Mac OS and Postgres 16 the module will have .dylib extension
  # Instead of .so, so any of the files may not exist
  # So we will ignore the error from cp command
  cp ${LIB_BUILD_DIR}/*.{so,dylib} ${OUT_DIR}/${PACKAGE_NAME}/lib 2>/dev/null || true
  
  cp ${SOURCE_DIR}/README.md ${OUT_DIR}/${PACKAGE_NAME}/ 
  cp ${SOURCE_DIR}/LICENSE ${OUT_DIR}/${PACKAGE_NAME}/ 2>/dev/null || true
  cp ${SOURCE_DIR}/scripts/packaging/* ${OUT_DIR}/${PACKAGE_NAME}/
  cp ${SHARE_BUILD_DIR}/*.sql ${OUT_DIR}/${PACKAGE_NAME}/lib
  cp ${SHARE_BUILD_DIR}/*.control ${OUT_DIR}/${PACKAGE_NAME}/lib

  pushd "$OUT_DIR"
    tar cf ${PACKAGE_NAME}.tar ${PACKAGE_NAME}
    ## Write output so we can use this in actions and upload artifacts
    echo "archive_package_name=${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
    echo "archive_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
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

function configure_and_start_postgres() {
  PGDATA=/etc/postgresql/$PG_VERSION/main
  # Enable auth without password
  echo "local   all             all                                     trust" >  $PGDATA/pg_hba.conf
  echo "host    all             all             127.0.0.1/32            trust" >>  $PGDATA/pg_hba.conf
  echo "host    all             all             ::1/128                 trust" >>  $PGDATA/pg_hba.conf
  # Set port
  echo "port = 5432" >> ${PGDATA}/postgresql.conf
  # Start postgres
  sudo service postgresql start
  
  wait_for_pg

  psql -U postgres -c "CREATE EXTENSION lantern" postgres
}

setup_environment && \
setup_locale_and_install_packages && \
setup_rust

if [ ! -z "$PACKAGE_CLI" ]
then
 package_cli
fi

if [ ! -z "$SETUP_POSTGRES" ]
then
 setup_postgres
fi
  
if [ ! -z "$SETUP_TESTS" ]
then
  setup_lantern && \
  setup_onnx && \
  configure_and_start_postgres
fi

if [ ! -z "$PACKAGE_EXTENSION" ]
then
 setup_cargo_deps && \
 package_extension
fi
