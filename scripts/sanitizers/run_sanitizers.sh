#!/bin/bash
if [ ! -f src/hnsw.c ]; then
      echo "script must be run in lantern root directory"
      exit 1
fi

mkdir -p sanitizer

docker build -t lantern-san -f scripts/sanitizers/Dockerfile .

function kill-docker {
    docker kill lantern-sanitizers
}

trap kill-docker EXIT

docker run --rm -d -v $(pwd)/sanitizer:/lantern/sanitizer --name lantern-sanitizers lantern-san

docker exec -i -u root lantern-sanitizers /bin/bash <<EOF
chown -R postgres:postgres /lantern/sanitizer
EOF

docker exec -i -u postgres -w /lantern/build lantern-sanitizers /bin/bash <<EOF
make test
cp /tmp/lantern/tmp_output/results/*.out /lantern/sanitizer
EOF
