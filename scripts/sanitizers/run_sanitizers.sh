#!/bin/bash
u_flag="true"
v_flag=""
if [ ! -f src/hnsw.c ]; then
      echo "script must be run in lantern root directory"
      exit 1
fi

function print_usage {
    printf "FLAGS:
    '-u' | run the container with ubsan enabled. This may take a long time, expect testing take about 30m
    '-v <version>' | the version of postgres you wish to test against, by default 15.4. Must include minor version\n"
}

while getopts ':uv:' flag; do
  case "${flag}" in
    u) u_flag="true" ;;
    v) v_flag="$OPTARG" ;;
    *) print_usage
       exit 1 ;;
  esac
done

function kill_docker {
    docker kill lantern-sanitizers
}

trap kill_docker EXIT

mkdir -p sanitizer

CONTAINER=""
ARGS=""
if [[ ! -z $v_flag ]]; then
    if [[ $v_flag =~ [0-9]{2}\.[0-9]{1,2} ]]; then
        CONTAINER="-$v_flag"
        ARGS="--build-arg VERSION=$v_flag"
    else
        echo "please specify a valid version"
        exit 1
    fi
fi
if [[ "$u_flag" == "true" ]]; then
    ARGS="$ARGS --build-arg UBSAN=,undefined"
    CONTAINER="lantern-san-ub$CONTAINER"
else
    CONTAINER="lantern-san$CONTAINER"
fi

docker build -t $CONTAINER -f scripts/sanitizers/Dockerfile $ARGS .

docker run --rm -d -v $(pwd)/sanitizer:/lantern/sanitizer --name lantern-sanitizers $CONTAINER

docker exec -i -u root lantern-sanitizers /bin/bash <<EOF
chown -R postgres:postgres /lantern/sanitizer
EOF

DIFF_PATH=/tmp/lantern/tmp_output/regression.diffs
docker exec -i -u postgres -w /lantern/build lantern-sanitizers /bin/bash <<EOF
make test
if test -f $DIFF_PATH; then
    cp /tmp/lantern/tmp_output/regression.diffs /lantern/sanitizer/test.diffs
fi
make test-parallel
if test -f $DIFF_PATH; then
    cp /tmp/lantern/tmp_output/regression.diffs /lantern/sanitizer/test-parallel.diffs
fi
EOF
