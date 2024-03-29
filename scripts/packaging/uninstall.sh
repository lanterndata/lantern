#!/bin/bash

if [ -z "$PG_CONFIG" ]
then
  if ! command -v pg_config &> /dev/null
  then
    echo "pg_config could not be found. Please specify with PG_CONFIG env variable"
    exit 1
  fi
  PG_CONFIG=$(which pg_config)
fi

PG_LIBRARY_DIR=$($PG_CONFIG --pkglibdir)
PG_EXTENSION_DIR=$($PG_CONFIG --sharedir)/extension

rm -rf $PG_LIBRARY_DIR/lantern*.so
rm -rf $PG_LIBRARY_DIR/bitcode/lantern 2> /dev/null || true
rm -rf $PG_LIBRARY_DIR/bitcode/lantern.index.bc 2> /dev/null || true
rm -rf $PG_EXTENSION_DIR/lantern*.sql
rm -rf $PG_EXTENSION_DIR/lantern*.control

echo "Lantern uninstalled successfully"
