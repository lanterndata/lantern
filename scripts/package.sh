#!/bin/bash

cd $BUILD_DIR
ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}')
PACKAGE_NAME=lanterndb-${EXT_VERSION}-${ARCH}

mkdir -p ${BUILD_DIR}/${PACKAGE_NAME}/src
mkdir ${BUILD_DIR}/${PACKAGE_NAME}/cmake
cp ${SOURCE_DIR}/cmake/PackageExtensionTemplate.cmake ${BUILD_DIR}/${PACKAGE_NAME}/CMakeLists.txt
cp ${SOURCE_DIR}/cmake/FindPostgreSQL.cmake ${BUILD_DIR}/${PACKAGE_NAME}/cmake
cp ${BUILD_DIR}/*.so ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${BUILD_DIR}/*.sql ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${SOURCE_DIR}/sql/updates/*.sql ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${SOURCE_DIR}/lanterndb.control ${BUILD_DIR}/${PACKAGE_NAME}/src

cd ${BUILD_DIR} && tar cf ${PACKAGE_NAME}.tar ${PACKAGE_NAME}
rm -rf ${BUILD_DIR}/${PACKAGE_NAME}

## Write output so we can use this in actions and upload artifacts
echo "archive_package_name=${PACKAGE_NAME}.tar" >> "/tmp/gh-output.txt"
echo "archive_package_path=${BUILD_DIR}/${PACKAGE_NAME}.tar" >> "/tmp/gh-output.txt"
