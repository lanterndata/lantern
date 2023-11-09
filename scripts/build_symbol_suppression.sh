#!/bin/bash

# find exports of the form 'PGDLLEXPORT PGFUNCTION_INFO_V1(function_name)'
EXPORTS=$(grep -F -r 'PGDLLEXPORT' $1 | grep -oP 'PG_FUNCTION_INFO_V1\(\K[^\)]+')

# find exports of the form 'PGDLLEXPORT <type> function_name(...)'
EXPORTS2=$(grep -roE 'PGDLLEXPORT \w+ \w+' $1 | awk '{print $NF}')

# concatenate the two groups
EXPORTS="$EXPORTS
$EXPORTS2"

# remove duplicates
SYMBOLS=$(echo "$EXPORTS" | sort -u | tr '\n' ';')

# build version script 
# this script will place everything except undefined symbols and 
echo "LANTERN {
global: Pg_magic_func; pg_finfo*; $SYMBOLS
local: *;
};" > $2
