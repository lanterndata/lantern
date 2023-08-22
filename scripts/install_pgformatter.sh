#!/bin/bash

# Exit if any command fails
set -e

# Set the version of pgFormatter
PGFORMATTER_VERSION=5.3

# Update package lists
apt update -y

# Install required dependencies for pgFormatter
apt install -y perl make gcc libcgi-pm-perl

# Download and extract pgFormatter
wget https://github.com/darold/pgFormatter/archive/refs/tags/v${PGFORMATTER_VERSION}.tar.gz
tar xzf v${PGFORMATTER_VERSION}.tar.gz

# Navigate to the extracted directory
pushd pgFormatter-${PGFORMATTER_VERSION}/

# Build and install pgFormatter
perl Makefile.PL
make
make install

# Navigate back to the previous directory
popd

# Clean up downloaded tarball and extracted directory
rm -rf v${PGFORMATTER_VERSION}.tar.gz pgFormatter-${PGFORMATTER_VERSION}
