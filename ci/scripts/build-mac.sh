#!/bin/bash
set -e

function setup_locale_and_install_packages() {
  export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"
  export C_INCLUDE_PATH=/usr/local/include
  export CPLUS_INCLUDE_PATH=/usr/local/include
}

function setup_postgres() {
  cmd="brew install postgresql@${PG_VERSION} clang-format || true" # ignoring brew linking errors
  if [[ $USER == "root" ]]
  then
    # Runner is github CI user
    sh -c "sudo -u runner -i $cmd"
    sh -c "sudo -u runner -i brew reinstall gettext"
    sh -c "sudo -u runner -i brew unlink gettext"
    sh -c "sudo -u runner -i brew link gettext --force"
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

