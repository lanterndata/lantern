#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

SCRIPT=$(realpath "$0")
THIS_DIR=$(dirname "$SCRIPT")

# get all the symbols our shared library assumes are externally provided
MAYBE_EXTERN=$(nm -u $1 | awk '{print $2}' | sed  -e 's/@/@@/')

# get all the symbols that are externally provided
EXTERN_PROVIDED=$($THIS_DIR/extern_defined.sh)

# get all the symbols that we assume are externally provided, while the are not
# -P needed in grep for grepping TABs
# https://stackoverflow.com/questions/10038188/searching-tabs-with-grep
# grep returns exit code 1 on no match, which is why we use || true
MISSING_SYMBOLS=$(comm -3 <(sort <<< $MAYBE_EXTERN) <(sort <<< $EXTERN_PROVIDED) | grep -P -v '\t' || true)

EXIT_CODE=0
for s in $MISSING_SYMBOLS
do
	EXIT_CODE=1
	printf "MISSING SYMBOL!!!\t $s\n"
done

exit $EXIT_CODE
