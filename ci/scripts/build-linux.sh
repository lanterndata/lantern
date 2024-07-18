#!/bin/bash
set -e

function setup_locale_and_install_packages() {
  echo "LC_ALL=en_US.UTF-8" > /etc/environment
  echo "CC=/usr/bin/gcc-12" >> /etc/environment
  echo "CXX=/usr/bin/g++-12" >> /etc/environment
  echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
  echo "LANG=en_US.UTF-8" > /etc/locale.conf

  source /etc/environment
  apt update -y
  apt install -y locales lsb-core build-essential automake cmake wget git dpkg-dev lcov clang-format clang llvm

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
  # preload pg_cron, necessary for async tasks test
  echo "shared_preload_libraries = 'pg_cron' " >> /etc/postgresql/$PG_VERSION/main/postgresql.conf
}

function setup_rust() {
  curl -k -o /tmp/rustup.sh https://sh.rustup.rs
  chmod +x /tmp/rustup.sh
  /tmp/rustup.sh -y --default-toolchain=1.78.0
  . "$HOME/.cargo/env"
}

function install_platform_specific_dependencies() {
  # Currently lantern_extras binaries are only available for Linux x86_64
  # We won't install onnxruntime as lantern_extras are used only for external index in tests
  pushd /tmp
    LANTERN_EXTRAS_VERSION=$(curl -s "https://api.github.com/repos/lanterndata/lantern_extras/releases/latest" | jq ".tag_name" | sed 's/"//g')
    wget --quiet https://github.com/lanterndata/lantern_extras/releases/download/${LANTERN_EXTRAS_VERSION}/lantern-extras-${LANTERN_EXTRAS_VERSION}.tar -O lantern-extras.tar
    tar xf lantern-extras.tar
    pushd lantern-extras-${LANTERN_EXTRAS_VERSION}
      make install
    popd
    rm -rf lantern-extras*

    # check needed to make sure double-cloning does not happen on the benchmarking CI/CD instance
    if [ ! -d "pg_cron" ]
    then
      git clone https://github.com/citusdata/pg_cron.git
    fi

    pushd pg_cron
      git fetch
      git checkout ${PG_CRON_COMMIT_SHA}
      make -j && make install
    popd

    apt install -y ruby-full
    gem install bundler

    # We need lantern-cli only for tests
    if [[ "$INSTALL_CLI" = "1" ]]
    then
      setup_rust
      # TODO:: change branch after PR merge
      ORT_STRATEGY=system cargo install --git https://github.com/lanterndata/lantern_extras.git --branch varik/external-indexing-server --debug --bin lantern-cli --root /tmp
    fi

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
  if [[ "$ENABLE_COVERAGE" != "1" ]]
  then
    /tmp/lantern/scripts/check_symbols.sh ./lantern.so
  fi

  # Chown to postgres for running tests
  chown -R postgres:postgres /tmp/lantern
  chown -R postgres:postgres /tmp/pgvector
}
