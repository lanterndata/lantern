# Define the PostgreSQL version argument
ARG PG_VERSION=15

# Set the base image using the provided PostgreSQL version
FROM postgres:$PG_VERSION-bookworm
ARG PG_VERSION

# Set the working directory in the image
WORKDIR /tmp/lanterndb

# Copy the local content into the working directory
COPY . .

# Run the build script with the specified PostgreSQL version
RUN PG_VERSION=$PG_VERSION ./ci/scripts/build-docker.sh 