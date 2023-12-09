#!/bin/bash
set -e

function setup_locale_and_install_packages() {
  export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"
  export C_INCLUDE_PATH=/usr/local/include
  export CPLUS_INCLUDE_PATH=/usr/local/include
}

function setup_postgres() {
  # This causes brew to error (Error: The `brew link` step did not complete successfully)
  # Removing the symlink to avoid error
  rm -rf '/usr/local/bin/2to3-3.12' || true
  
  cmd="brew install --overwrite postgresql@${PG_VERSION} clang-format"
  if [[ $USER == "root" ]]
  then
    # Runner is github CI user
    sh -c "sudo -u runner -i $cmd"
  else
    sh -c $cmd
  fi
}

function package_if_necessary() {
  :
  # TODO make and publish homebrew formula
}

function cleanup_environment() {
  # Add permission to for running tests from runner user
  chmod -R 777 /tmp/lantern
}

