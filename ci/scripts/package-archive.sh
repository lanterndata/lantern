#!/bin/bash

GITHUB_OUTPUT=${GITHUB_OUTPUT:-/dev/null}

cd /tmp/lantern/lantern_hnsw/build && make archive
cat /tmp/gh-output.txt >> $GITHUB_OUTPUT && rm -rf /tmp/gh-output
