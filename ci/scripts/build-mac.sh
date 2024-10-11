#!/bin/bash
set -e

function setup_locale_and_install_packages() {
  export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"
  export C_INCLUDE_PATH=/usr/local/include
  export CPLUS_INCLUDE_PATH=/usr/local/include

  export CC=/usr/bin/clang
  export CXX=/usr/bin/clang++
}

function setup_cargo_deps() {
  if [ ! -d .cargo ]; then
  	mkdir .cargo
  fi

  echo "[target.$(rustc -vV | sed -n 's|host: ||p')]" > .cargo/config
  echo 'rustflags = ["-Clink-arg=-Wl,-undefined,dynamic_lookup"]' >> .cargo/config

  cargo install cargo-pgrx --version 0.11.3
  cargo pgrx init "--pg$PG_VERSION" $(which pg_config)
}

function setup_postgres() {
  cmd="brew install postgresql@${PG_VERSION} clang-format || true" # ignoring brew linking errors
  if [[ $USER == "root" ]]
  then
    # Runner is github CI user
    sh -c "sudo -u runner -i $cmd"
    sh -c "sudo -u runner -i brew reinstall gettext || true"
    sh -c "sudo -u runner -i brew unlink gettext || true"
    sh -c "sudo -u runner -i brew link gettext --force || true"
  else
    sh -c $cmd
  fi
}

function install_platform_specific_dependencies() {
  :
}

function package_if_necessary() {
  :
}

function cleanup_environment() {
  # Add permission to for running tests from runner user
  chmod -R 777 /tmp/lantern
}

