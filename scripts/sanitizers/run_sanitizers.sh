#!/bin/bash
u_flag=''
if [ ! -f src/hnsw.c ]; then
      echo "script must be run in lantern root directory"
      exit 1
fi

function print_usage {
    printf "This script takes 1 option '-u' to run the container with ubsan enabled. This may take a long time, expect testing take about 30m\n"
}

while getopts 'u' flag; do
  case "${flag}" in
    u) u_flag='true' ;;
    *) print_usage
       exit 1 ;;
  esac
done

function kill_docker {
    docker kill lantern-sanitizers
}

trap kill_docker EXIT

mkdir -p sanitizer

CONTAINER=''
if [[ "$u_flag" == "true" ]]; then
    docker build -t lantern-san-ub -f scripts/sanitizers/Dockerfile --build-arg="UBSAN=,undefined" .
    CONTAINER='lantern-san-ub'
else
    docker build -t lantern-san -f scripts/sanitizers/Dockerfile .
    CONTAINER='lantern-san'
fi

docker run --rm -d -v $(pwd)/sanitizer:/lantern/sanitizer --name lantern-sanitizers $CONTAINER

docker exec -i -u root lantern-sanitizers /bin/bash <<EOF
chown -R postgres:postgres /lantern/sanitizer
EOF

docker exec -i -u postgres -w /lantern/build lantern-sanitizers /bin/bash <<EOF
make test
cp /tmp/lantern/tmp_output/results/*.out /lantern/sanitizer
EOF
