#!/bin/bash

export GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}

PACKAGE_DIR=/tmp/lantern-extras-package
PACKAGE_VERSION=$(ls -t $PACKAGE_DIR | head -1 | sed -E "s#lantern-extras-(.*)-postgres.*#\1#")
PACKAGE_NAME=lantern-extras-${PACKAGE_VERSION}
OUTPUT_DIR=/tmp/$PACKAGE_NAME
SHARED_DIR=${OUTPUT_DIR}/shared
mkdir $OUTPUT_DIR

cd $PACKAGE_DIR
for f in $(find "." -name "*.tar"); do
    current_archive_name=$(echo $f | sed -E 's#(.*).tar#\1#')   
    current_pg_version=$(echo $current_archive_name | sed -E 's#(.*)-postgres-(.*)-(.*)-(.*)#\2#')   
    current_platform=$(echo $current_archive_name | sed -E 's#(.*)-postgres-(.*)-(.*)-(.*)#\3#')
    current_arch=$(echo $current_archive_name | sed -E 's#(.*)-postgres-(.*)-(.*)-(.*)#\4#')   
    current_dest_folder=${OUTPUT_DIR}/lib/${current_arch}/${current_platform}/${current_pg_version}

    tar xf $f

    if [ ! -d "$SHARED_DIR" ]; then
      # Copying static files which does not depend to architecture and pg version only once
      mkdir -p $SHARED_DIR
      cp $current_archive_name/Makefile $OUTPUT_DIR/
      cp $current_archive_name/README.md $OUTPUT_DIR/
      cp $current_archive_name/LICENSE $OUTPUT_DIR/ 2>/dev/null || true
      cp $current_archive_name/*.sh $OUTPUT_DIR/
      cp $current_archive_name/lib/*.sql $SHARED_DIR/
      cp $current_archive_name/lib/*.control $SHARED_DIR/
    fi

    mkdir -p $current_dest_folder
    cp $current_archive_name/lib/*.{so,dylib} $current_dest_folder/ 2>/dev/null || true
done

cd /tmp && tar cf ${PACKAGE_NAME}.tar $PACKAGE_NAME
echo "package_name=${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
echo "package_path=/tmp/${PACKAGE_NAME}.tar" >> $GITHUB_OUTPUT
echo "package_version=${PACKAGE_VERSION}" >> $GITHUB_OUTPUT
