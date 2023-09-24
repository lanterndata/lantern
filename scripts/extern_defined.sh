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
SED_PATTERN='s/@/@/p' # noop pattern
# " T " - text symbol
nm -D --with-symbol-versions $PG_BIN | grep " T " | awk '{print $3}' | sed -e "$SED_PATTERN"
# global bss symbol in postgres
nm -D --with-symbol-versions $PG_BIN | grep " B " | awk '{print $3}' | sed -e "$SED_PATTERN"
# postgres Initialized data (bbs), global symbols
nm -D --with-symbol-versions $PG_BIN | grep " D " | awk '{print $3}' | sed -e "$SED_PATTERN"
# postgres weak symbols
nm -D --with-symbol-versions $PG_BIN | grep " w " | awk '{print $2}' | sed -e "$SED_PATTERN"

# Get a list of shared library dependencies using ldd
dependencies=$(ldd "$PG_BIN" | awk '{print $3}' | grep -v "not a dynamic executable")

# Loop through the dependencies and extract symbols
for dependency in $dependencies; do
   SED_PATTERN='s/@/@/p' # noop pattern
   if grep -q "libstdc++" <<< "$dependency"; then
      # even if postgres is linked against libstdc++, we should not use those and should
      # always have our statically linked libstdc++ as postgres may not always be linked
      # against libstdc++
      continue
   fi

   if grep -q "libm" <<< "$dependency"; then
      #libm does not use symbol versioning
	   SED_PATTERN='s/@.*$//p'
   fi
   # " U " - undefined symbol
   nm -D --with-symbol-versions "$dependency" | awk '/ U / {print $3}' | sed -e "$SED_PATTERN"
   # " i " - the symbol is an indirect reference to another symbol. This is often used for compiler-generated code
   nm -D --with-symbol-versions "$dependency" | awk '/ i / {print $3}' | sed -e "$SED_PATTERN"
   # " T " - The symbol is a text (code) symbol, representing a function or code that can be executed
   nm -D --with-symbol-versions "$dependency" | awk '/ T / {print $3}' | sed -e "$SED_PATTERN"
   # " V " - the symbol is a weak object
   nm -D --with-symbol-versions "$dependency" | awk '/ V / {print $3}' | sed -e "$SED_PATTERN"
   # " W " - the symbol is a weak symbol that has not been specifically tagged as weak object symbol
   nm -D --with-symbol-versions "$dependency" | awk '/ W / {print $3}' | sed -e "$SED_PATTERN"
   # " B " global bss symbol. e.g. __libc_single_threaded@@GLIBC_2.32
   nm -D --with-symbol-versions "$dependency" | awk '/ B / {print $3}' | sed -e "$SED_PATTERN"
   # " D " weak symbol. e.g. stderr@@GLIBC_2.2.5
   nm -D --with-symbol-versions "$dependency" | awk '/ D / {print $3}' | sed -e "$SED_PATTERN"
done

# We link libstdc++ statically and it uses the symbol below from ld-linux
# Now we need to add ld-linux symbols to extern_defined. Since this is the only symbol we use,
# we can just filter and add only that one
# " T " text symbol. e.g. __tls_get_addr
LD_LINUX=$(ldd $(pg_config --bindir)/postgres| grep ld-linux | awk '{print $1}')
nm -D --with-symbol-versions "$LD_LINUX" | awk '/ T / {print $3}' | sed -e "$SED_PATTERN" | grep __tls_get_addr
