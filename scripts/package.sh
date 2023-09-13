#!/bin/bash

cd $BUILD_DIR
ARCH=$(dpkg-architecture -q DEB_BUILD_ARCH)
EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}')
PACKAGE_NAME=lantern-${EXT_VERSION}-postgres-${PG_VERSION}-${ARCH}

mkdir -p ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${SOURCE_DIR}/scripts/packaging/* ${BUILD_DIR}/${PACKAGE_NAME}/
cp ${BUILD_DIR}/*.so ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${BUILD_DIR}/*.sql ${BUILD_DIR}/${PACKAGE_NAME}/src

for f in $(find "${SOURCE_DIR}/sql/updates/" -name "*.sql"); do
    dest_filename=$(echo $f | sed -E 's#(.*)/(.*\.sql)#lantern--\2#g')
    cp $f ${BUILD_DIR}/${PACKAGE_NAME}/src/${dest_filename}
done

cp ${BUILD_DIR}/lantern.control ${BUILD_DIR}/${PACKAGE_NAME}/src

cd ${BUILD_DIR} && tar cf ${PACKAGE_NAME}.tar ${PACKAGE_NAME}
rm -rf ${BUILD_DIR}/${PACKAGE_NAME}

## Write output so we can use this in actions and upload artifacts
echo "archive_package_name=${PACKAGE_NAME}.tar" >> "/tmp/gh-output.txt"
echo "archive_package_path=${BUILD_DIR}/${PACKAGE_NAME}.tar" >> "/tmp/gh-output.txt"
