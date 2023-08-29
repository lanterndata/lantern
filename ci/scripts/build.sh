#!/bin/bash

function setup_environment() {
  export DEBIAN_FRONTEND=noninteractive
  export PG_VERSION=${PG_VERSION:-15}
  export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}
  export ORT_STRATEGY=system
  export ORT_LIB_LOCATION=/usr/local/lib/onnxruntime
}

function install_onnx_runtime(){
  ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
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
}

function setup_postgres() {
  # Add postgresql apt repo
  export ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
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

function package_extension() {
  cargo pgrx package --pg-config /usr/bin/pg_config --package lanterndb_extras

  EXT_VERSION=$(cargo metadata --format-version 1 | jq '.packages[] | select( .name == "lanterndb_extras") | .version' | tr -d '"')
  PACKAGE_NAME=lanterndb-extras-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}

  SOURCE_DIR=$(pwd)
  LIB_BUILD_DIR="$(pwd)/target/release/lanterndb_extras-pg${PG_VERSION}/usr/lib/postgresql/${PG_VERSION}/lib"
  SHARE_BUILD_DIR="$(pwd)/target/release/lanterndb_extras-pg${PG_VERSION}/usr/share/postgresql/${PG_VERSION}/extension"
  OUT_DIR=/tmp/lanterndb-extras
  mkdir -p ${OUT_DIR}/${PACKAGE_NAME}/src

  cp ${SOURCE_DIR}/scripts/packaging/* ${OUT_DIR}/${PACKAGE_NAME}/
  cp ${LIB_BUILD_DIR}/*.so ${OUT_DIR}/${PACKAGE_NAME}/src
  cp ${SHARE_BUILD_DIR}/*.sql ${OUT_DIR}/${PACKAGE_NAME}/src
  cp ${SHARE_BUILD_DIR}/*.control ${OUT_DIR}/${PACKAGE_NAME}/src

  cd ${OUT_DIR} && tar cf ${PACKAGE_NAME}.tar ${PACKAGE_NAME}
  rm -rf ${BUILD_DIR}/${PACKAGE_NAME}
  ## Write output so we can use this in actions and upload artifacts
  echo "archive_package_name=${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
  echo "archive_package_path=${OUT_DIR}/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
}

setup_environment && \
setup_locale_and_install_packages && \
setup_postgres && \
setup_rust && \
install_onnx_runtime && \
setup_cargo_deps && \
package_extension

