#!/bin/bash
if [ ! -f src/hnsw.c ]; then
      echo "script must be run in lantern root directory"
      exit 1
fi
docker build -t lantern-san -f scripts/sanitizers/Dockerfile .
docker run lantern-san

docker exec -i -u postgres -w /lantern/build lantern-san /bin/bash <<EOF
make test
EOF
