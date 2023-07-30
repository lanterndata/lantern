#!/bin/bash

if [ -z "$GITHUB_OUTPUT" ]
then
  export GITHUB_OUTPUT=/dev/null
fi

cd /tmp/lanterndb/build && make archive
cat /tmp/gh-output.txt >> $GITHUB_OUTPUT && rm -rf /tmp/gh-output
