#!/bin/bash
set -e

# This sets $ARCH and $PLATFORM env variables
source "$(dirname "$0")/../../lantern_hnsw/scripts/get_arch_and_platform.sh"

if [[ $PLATFORM == "mac" ]]; then
   BUILD_SCRIPT="build-mac.sh"
elif [[ $PLATFORM == "linux" ]]; then
   BUILD_SCRIPT="build-linux.sh"
else
   echo "Invalid target use one of [mac, linux]"
   exit 1
fi

source "$(dirname "$0")/utils.sh"

setup_environment

# Source platform specific build script
source "$(dirname "$0")/${BUILD_SCRIPT}"

setup_locale_and_install_packages
setup_postgres
install_external_dependencies
clone_or_use_source
install_platform_specific_dependencies
build_and_install_lantern
package_if_necessary
cleanup_environment
