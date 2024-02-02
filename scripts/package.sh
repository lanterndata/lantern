#!/bin/bash

source "$(dirname "$0")/get_arch_and_platform.sh"
cd $BUILD_DIR
EXT_VERSION=$(cmake --system-information | awk -F= '$1~/CMAKE_PROJECT_VERSION:STATIC/{print$2}')
PACKAGE_NAME=lantern-${EXT_VERSION}-postgres-${PG_VERSION}-${PLATFORM}-${ARCH}

mkdir -p ${BUILD_DIR}/${PACKAGE_NAME}/src
cp ${SOURCE_DIR}/scripts/packaging/* ${BUILD_DIR}/${PACKAGE_NAME}/

# For Mac OS and Postgres 16 the module will have .dylib extension
# Instead of .so, so any of the files may not exist
# So we will ignore the error from cp command
cp ${BUILD_DIR}/*.{so,dylib} ${BUILD_DIR}/${PACKAGE_NAME}/src 2>/dev/null || true
cp -r ${BUILD_DIR}/bitcode ${BUILD_DIR}/${PACKAGE_NAME}/src/bitcode 2>/dev/null || true
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
