#!/bin/bash

function setup_environment() {
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
  export ORT_STRATEGY=system
  export ORT_DYLIB_PATH=/usr/local/lib/onnxruntime
}

function install_onnx_runtime(){
  PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v1.15.1/onnxruntime-linux-x64-1.15.1.tgz"
  if [[ $ARCH == *"arm"* ]]; then
    PACKAGE_URL="https://github.com/microsoft/onnxruntime/releases/download/v1.15.1/onnxruntime-linux-aarch64-1.15.1.tgz"
  fi

  mkdir -p /usr/local/lib
  pushd /usr/local/lib
  wget $PACKAGE_URL && \
  tar xzf ./onnx*.tgz && \
  rm -rf ./onnx*.tgz && \
  mv ./onnx* ./onnxruntime && \
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
  echo 'rustflags = ["-C", "link-args=-Wl,-rpath,/usr/local/lib/onnxruntime/lib"]' >> .cargo/config
  cargo install cargo-pgrx --version 0.9.7
  cargo pgrx init "--pg$PG_VERSION" /usr/bin/pg_config
}

function package_create_index() {
  VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lantern_create_index") | .version' | tr -d '"')
  PACKAGE_NAME=lantern-create-index-${VERSION}-${ARCH}
  SOURCE_DIR=$(pwd)
  BINARY_NAME=lantern-create-index
  OUT_DIR=/tmp/${BINARY_NAME}
  BUILD_DIR=${SOURCE_DIR}/target/release/
  
  cargo build --package lantern_create_index --release 

  mkdir -p ${OUT_DIR}
  
  cp ${BUILD_DIR}/${BINARY_NAME} $OUT_DIR
  
  pushd "$OUT_DIR"
    tar cf ${PACKAGE_NAME}.tar $BINARY_NAME
    ## Write output so we can use this in actions and upload artifacts
    echo "create_index_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
  popd
}

function package_extension() {
  cargo pgrx package --pg-config /usr/bin/pg_config --package lantern_extras

  EXT_VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lantern_extras") | .version' | tr -d '"')
  PACKAGE_NAME=lantern-extras-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}

  SOURCE_DIR=$(pwd)
  LIB_BUILD_DIR="$(pwd)/target/release/lantern_extras-pg${PG_VERSION}/usr/lib/postgresql/${PG_VERSION}/lib"
  SHARE_BUILD_DIR="$(pwd)/target/release/lantern_extras-pg${PG_VERSION}/usr/share/postgresql/${PG_VERSION}/extension"
  OUT_DIR=/tmp/lantern-extras
  mkdir -p ${OUT_DIR}/${PACKAGE_NAME}/src

  cp ${SOURCE_DIR}/scripts/packaging/* ${OUT_DIR}/${PACKAGE_NAME}/
  cp ${LIB_BUILD_DIR}/*.so ${OUT_DIR}/${PACKAGE_NAME}/src
  cp ${SHARE_BUILD_DIR}/*.sql ${OUT_DIR}/${PACKAGE_NAME}/src
  cp ${SHARE_BUILD_DIR}/*.control ${OUT_DIR}/${PACKAGE_NAME}/src

  pushd "$OUT_DIR"
    tar cf ${PACKAGE_NAME}.tar ${PACKAGE_NAME}
    ## Write output so we can use this in actions and upload artifacts
    echo "archive_package_name=${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
    echo "archive_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
  popd
}

setup_environment && \
setup_locale_and_install_packages && \
setup_rust

if [ ! -z "$PACKAGE_CREATE_INDEX" ]
then
 package_create_index
else
 setup_postgres && \
 install_onnx_runtime && \
 setup_cargo_deps && \
 package_extension
fi

