#!/bin/bash

VERSION=$1
OUTPUT=$2

mkdir -p ${OUTPUT}/include

cat << EOF >> ${OUTPUT}/include/version.h
#ifndef LDB_HNSW_VERSION_H
#define LDB_HNSW_VERSION_H

#define LDB_BINARY_VERSION "$VERSION"

#endif
EOF
