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

##
# defined in the postgres binary
nm -D  /usr/lib/postgresql/14/bin/postgres | grep " T " | awk '{print $3}'
# global bss symbol in postgres
nm -D  /usr/lib/postgresql/14/bin/postgres | grep " B " | awk '{print $3}'
# postgres weak symbols
nm -D  /usr/lib/postgresql/14/bin/postgres | grep " w " | awk '{print $2}'

nm -D /lib/x86_64-linux-gnu/libc.so.6 |  grep " T "| awk '{print $3}'
nm -D /lib/x86_64-linux-gnu/libc.so.6 |  grep " i "| awk '{print $3}' # for memmove et al
nm -D /lib/x86_64-linux-gnu/libc.so.6 |  grep " W "| awk '{print $3}' # for munmap, fread et al

# cpp symbols
nm -D   /lib/x86_64-linux-gnu/libstdc++.so.6  | grep " T "| awk '{print $3}' | sed -n -e 's/@@.*$//p'

# for _ZNSt9basic_iosIcSt11char_traitsIcEE5clearESt12_Ios_Iostate et all, pulled through usearch
# todo: make sure this is not an indication of an issue
# m " V " - the symbol is a weak object
nm -D   /lib/x86_64-linux-gnu/libstdc++.so.6  | grep " V "| awk '{print $3}' | sed -n -e 's/@@.*$//p'
# m " W " - the symbol is a weak symbol that has not been specifically tagged as weak object symbol
nm -D   /lib/x86_64-linux-gnu/libstdc++.so.6  | grep " W "| awk '{print $3}' | sed -n -e 's/@@.*$//p'

# for Unwind_Resome
#  has @@GCC_3.0 and our version has @GCC_3.0
# but the undefined symbols in our lib are modified with s/@/@@/
nm -D /lib/x86_64-linux-gnu/libgcc_s.so.1 | grep ' T '  | awk '{print $3}'

# add all libm symbols
nm -D /lib/x86_64-linux-gnu/libm.so.6 | grep -v " U " | awk '{print $3}' | sed -n -e 's/@@.*$//p'


