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

if command -v uname &> /dev/null
then
  ARCH=$(uname -m)
elif command -v dpkg &> /dev/null
then
  ARCH=$(dpkg --print-architecture)
elif command -v apk &> /dev/null
then
  ARCH=$(apk --print-arch)
else
  echo "Could not detect system architecture. Please specify with ARCH env variable"
  exit 1
fi

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=mac;;
    *)          PLATFORM=${unameOut}
esac

PG_LIBRARY_DIR=$($PG_CONFIG --pkglibdir)
PG_EXTENSION_DIR=$($PG_CONFIG --sharedir)/extension
PG_VERSION_STRING=$($PG_CONFIG --version)
PG_VERSION=$(echo $PG_VERSION_STRING | sed -E "s#^PostgreSQL ([0-9]+).*#\1#")

if [ ! -d src/${ARCH} ]
then
  echo "Architecture $ARCH not supported. Try building from source"
  exit
fi

if [ ! -d src/${ARCH}/${PLATFORM} ]
then
  echo "Platform $PLATFORM not supported. Try building from source"
  exit
fi

if [ ! -d src/${ARCH}/${PLATFORM}/${PG_VERSION} ]
then
  echo "Postgres version $PG_VERSION not supported"
  exit
fi

cp -r src/${ARCH}/${PLATFORM}/${PG_VERSION}/*.so $PG_LIBRARY_DIR
cp -r shared/*.sql $PG_EXTENSION_DIR
cp -r shared/*.control $PG_EXTENSION_DIR

echo "LanternDB installed successfully"

EXTRAS_PACKAGE_NAME=$(find . -name "lantern-extras*" | head -n 1)

if [ ! -z "$EXTRAS_PACKAGE_NAME" ]
then
  echo "Installing LanternDB Extras"
  cd $EXTRAS_PACKAGE_NAME && make install
fi
