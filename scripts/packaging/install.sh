#!/bin/bash

if [ -z "$PG_CONFIG" ]
then
  if ! command -v pg_config &> /dev/null
  then
    echo "pg_config could not be found. Please specify with PG_CONFIG env variable"
    exit
  fi
  PG_CONFIG=$(which pg_config)
fi

if [ -z "$ARCH" ]
then
  if command -v dpkg &> /dev/null
  then
    ARCH=$(dpkg --print-architecture)
  elif command -v apk &> /dev/null
  then
    ARCH=$(apk --print-arch)
  elif command -v uname &> /dev/null
  then
    ARCH=$(uname -m)
  else
    echo "Could not detect system architecture. Please specify with ARCH env variable"
    exit
  fi
fi

PG_LIBRARY_DIR=$($PG_CONFIG --pkglibdir)
PG_EXTENSION_DIR=$($PG_CONFIG --sharedir)/extension
PG_VERSION_STRING=$($PG_CONFIG --version)
PG_VERSION=$(echo $PG_VERSION_STRING | sed -E "s#^PostgreSQL ([0-9]+).*#\1#")

if [ ! -d src/${ARCH} ]
then
  echo "Architecture $ARCH not supported. Try building from source"
  exit
fi

if [ ! -d src/${ARCH}/${PG_VERSION} ]
then
  echo "Postgres version $PG_VERSION not supported"
  exit
fi

cp -r src/${ARCH}/${PG_VERSION}/*.so $PG_LIBRARY_DIR
cp -r shared/*.sql $PG_EXTENSION_DIR
cp -r shared/*.control $PG_EXTENSION_DIR

echo "LanternDB Extras installed successfully"
