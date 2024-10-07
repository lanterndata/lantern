#!/bin/bash

set -e

function setup_onnx() {
  pushd /tmp
    ONNX_VERSION="1.16.1"
    PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ONNX_VERSION}/onnxruntime-linux-x64-${ONNX_VERSION}.tgz" && \
    if [[ $ARCH == *"arm"* ]]; then PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ONNX_VERSION}/onnxruntime-linux-aarch64-${ONNX_VERSION}.tgz"; fi && \
    mkdir -p /usr/local/lib && \
    cd /usr/local/lib && \
    wget -q $PACKAGE_URL && \
    tar xzf ./onnx*.tgz && \
    rm -rf ./onnx*.tgz && \
    mv ./onnx* ./onnxruntime && \
    echo /usr/local/lib/onnxruntime/lib > /etc/ld.so.conf.d/onnx.conf && \
    ldconfig
  popd
}

function package_cli() {
  source "$(dirname "$0")/get_arch_and_platform.sh"
  VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lantern_cli") | .version' | tr -d '"')
  PACKAGE_NAME=lantern-cli-${VERSION}-${PLATFORM}-${ARCH}
  SOURCE_DIR=$(pwd)
  BINARY_NAME=lantern-cli
  OUT_DIR=/tmp/${BINARY_NAME}
  BUILD_DIR=${SOURCE_DIR}/target/release/
  
  CC=$(which clang) cargo build --package lantern_cli --release 

  mkdir -p ${OUT_DIR}
  
  cp ${BUILD_DIR}/${BINARY_NAME} $OUT_DIR
  
  pushd "$OUT_DIR"
    tar cf ${PACKAGE_NAME}.tar $BINARY_NAME
    ## Write output so we can use this in actions and upload artifacts
    echo "cli_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
    echo "cli_package_name=${PACKAGE_NAME}" >> $GITHUB_OUTPUT
  popd
}

function install_extension() {
  cargo pgrx install --pg-config /usr/bin/pg_config --package lantern_extras
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


function configure_and_start_postgres() {
  # Start postgres
  sudo service postgresql start
  
  wait_for_pg

  psql -U postgres -c "CREATE EXTENSION lantern" postgres
  psql -U postgres -c "CREATE EXTENSION lantern_extras" postgres
}

# Source unified utility functions
source "$(dirname "$0")/utils.sh"
source "$(dirname "$0")/build-linux.sh"

if [ ! -z "$RUN_POSTGRES" ]
then
  configure_and_start_postgres
  exit 0
fi

setup_environment
setup_locale_and_install_packages
clone_or_use_source

if [ ! -z "$SETUP_POSTGRES" ]
then
 setup_postgres
 install_platform_specific_dependencies
fi

if [ ! -z "$PACKAGE_CLI" ]
then
 package_cli
fi

  
if [ ! -z "$SETUP_TESTS" ]
then
  build_and_install_lantern
  setup_onnx
fi

if [ ! -z "$PACKAGE_EXTENSION" ]
then
 setup_cargo_deps
 package_extension
fi
