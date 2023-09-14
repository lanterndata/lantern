#!/bin/bash
set -e

function setup_locale_and_install_packages() {
  export PATH="/usr/local/opt/postgresql@${PG_VERSION}/bin:$PATH"
  export C_INCLUDE_PATH=/usr/local/include
  export CPLUS_INCLUDE_PATH=/usr/local/include
}

function setup_postgres() {
  cmd="brew install postgresql@${PG_VERSION}"
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
  : # nop
}

