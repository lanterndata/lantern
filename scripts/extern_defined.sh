#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# The compiler errors when we link a binary without providing all the necessary symbols
# used in the library
# There is no error when building shared libraries since in the common case our shared library
# will use symbls defined elsewhere, which will be loaded by the executible that loads our shared
# library

# for example, in our postgres extension we use palloc() to allocate memory but we do not supply
# definition for it or link against a library providing a definition
# What and how palloc() is implemented is up to the system loading our shared library and they
# should provide a definition in runtime

# However, this means that we can easily assume that our runtime will provide some function
# when the runtime actually does not provide such function. This will result in an error
# *when there is an attemt to use the function * (not even when the library is loaded but
# when the undefined symbol is used)

# This script tries to make sure we never allow such undefined symbols make it into the codebase
# it pulls in all the symbols from postgres binary and the shared libraries postgres depends on,
# collects all the symbols in all of them and print them out

# our build system will then run this script, and subtract its outputs from the list of undefined
# symbols in our shared library.
# if there are remaining undefined libraries in our shared library, we will know we have a problem
# at compile time

# todo:: make this portable
# 1. Use pg_config or similar to locate postgres instead of hardcoding its path
# 2. Use grep or something else to parse shared libraries postgres depends on instead of
#     hardcoding their paths
# 3. use platorm specific equivalent of nm, ldd, awk

PG_BIN=$(pg_config --bindir)/postgres

# " T " - text symbol
nm -D  $PG_BIN | grep " T " | awk '{print $3}' | sed -e 's/@.*$//p'
# global bss symbol in postgres
nm -D  $PG_BIN | grep " B " | awk '{print $3}' | sed -e 's/@.*$//p'
# postgres weak symbols
nm -D  $PG_BIN | grep " w " | awk '{print $2}' | sed -e 's/@.*$//p'

# Get a list of shared library dependencies using ldd
dependencies=$(ldd "$PG_BIN" | awk '{print $3}' | grep -v "not a dynamic executable")

# Loop through the dependencies and extract symbols
for dependency in $dependencies; do
   # " U " - undefined symbol
   nm -D "$dependency" | awk '/ U / {print $3}' | sed -e 's/@.*$//p'
   # " i " - the symbol is an indirect reference to another symbol. This is often used for compiler-generated code
   nm -D "$dependency" | awk '/ i / {print $3}' | sed -e 's/@.*$//p'
   # " T " - The symbol is a text (code) symbol, representing a function or code that can be executed
   nm -D "$dependency" | awk '/ T / {print $3}' | sed -e 's/@.*$//p'
   # " V " - the symbol is a weak object
   nm -D "$dependency" | awk '/ V / {print $3}' | sed -e 's/@.*$//p'
   # " W " - the symbol is a weak symbol that has not been specifically tagged as weak object symbol
   nm -D "$dependency" | awk '/ W / {print $3}' | sed -e 's/@.*$//p'
done
