#!/bin/bash
set -e

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
  echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add -
  # Install postgres and dev files for C headers
  apt update
  apt install -y postgresql-$PG_VERSION postgresql-server-dev-$PG_VERSION
  # Fix pg_config (sometimes it points to wrong version)
  rm -f /usr/bin/pg_config && ln -s /usr/lib/postgresql/$PG_VERSION/bin/pg_config /usr/bin/pg_config
}

function install_platform_specific_dependencies() {
  # Currently lantern_extras binaries are only available for Linux x86_64
  # We won't install onnxruntime as lantern_extras are used only for external index in tests
  pushd /tmp
    LANTERN_EXTRAS_VERSION=0.0.6
    wget https://github.com/lanterndata/lantern_extras/releases/download/${LANTERN_EXTRAS_VERSION}/lantern-extras-${LANTERN_EXTRAS_VERSION}.tar -O lantern-extras.tar
    tar xf lantern-extras.tar
    pushd lantern-extras-${LANTERN_EXTRAS_VERSION}
      make install
    popd
    rm -rf lantern-extras*
  popd
}

function package_if_necessary() {
  if [ -n "$BUILD_PACKAGES" ]; then
    # Bundle debian packages
    cpack &&
    # Print package name to github output
    export EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}') && \
    export PACKAGE_NAME=lantern-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}.deb && \

    echo "deb_package_name=$PACKAGE_NAME" >> "$GITHUB_OUTPUT" && \
    echo "deb_package_path=$(pwd)/$(ls *.deb | tr -d '\n')" >> "$GITHUB_OUTPUT"
  fi
}

function cleanup_environment() {
  # Check for undefined symbols
  if [ ! -n "$ENABLE_COVERAGE" ]
  then
    /tmp/lantern/scripts/check_symbols.sh ./lantern.so
  fi

  # Chown to postgres for running tests
  chown -R postgres:postgres /tmp/lantern
  chown -R postgres:postgres /tmp/pgvector
}
