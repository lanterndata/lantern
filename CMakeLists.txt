cmake_minimum_required(VERSION 3.3)

set(LANTERN_VERSION 0.3.1)

project(
  LanternDB
  VERSION ${LANTERN_VERSION}
  LANGUAGES C CXX)

if (POLICY CMP0074)
  # us <PackageName>_ROOT variables from inside find_package calls
  cmake_policy(SET CMP0074 NEW)
endif()

if(POLICY CMP0077)
  # Allow parent project to override options of children obtain via FetchContent
  # or add_subdirectory.
  cmake_policy(SET CMP0077 NEW)
endif()

set(RELEASE_ID "latest" CACHE STRING "Release ID placed in the binary. Must be set externally when doing a release")

option(BUILD_FOR_DISTRIBUTING "Build LANTERN_VERSION info into the binary" OFF)
option(MARCH_NATIVE "Build assuming the presence of all the features in the current CPU model" ON)
option(USEARCH_USE_SIMSIMD "Build usearch with SIMSIMD" OFF)
option(POSTGRES_UBSAN "Turn this on when building with a postgres that has UBSAN turned on" OFF)

option(DEV "Developer mode: provide code formatting, get postgres source, etc." OFF)
option(CODECOVERAGE "Enable code coverage for the build" OFF)
option(BENCH "Enable benchmarking" OFF)
option(FAILURE_POINTS "Enable failure points" OFF)

if (${BUILD_FOR_DISTRIBUTING})
  set(RELEASE_ID ${LANTERN_VERSION})
endif()

if(CODECOVERAGE)
  message(STATUS "Code coverage is enabled.")
  # Note that --coverage is synonym for the necessary compiler and linker flags
  # for the given compiler.  For example, with GCC, --coverage translates to
  # -fprofile-arcs -ftest-coverage when compiling and -lgcov when linking
  add_compile_options(--coverage -O0)
  # add NDEBUG to make sure asserts compile to a NOOP and do not negatively impact
  # code coverage. Asserts check invariants but since 'assert' is defined as a macro-condition,
  # codecov always considers it partially covered.
  add_compile_definitions(NDEBUG)
  add_link_options(--coverage)
endif(CODECOVERAGE)

# general flags applicible to everything
# details: https://lwn.net/Articles/584225/
add_compile_options("-fstack-protector-strong")

if (BENCH)
  message(STATUS "Benchmarking is enabled.")
  add_compile_definitions(LANTERN_BENCH)
endif()

# options passed into lantern sourcecode
# todo:: tests for copynodes=ON are broken
option(LANTERNDB_COPYNODES "Copy postgres index tuples for external retriever during scan instead of pinning" OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
message(STATUS "${CMAKE_COLOR_GREEN}Build type: ${CMAKE_BUILD_TYPE}${CMAKE_COLOR_RESET}")

# Set the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PostgreSQL REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SOURCES_DIR "src")
# todo:: list out all the files instead of glob
file(GLOB_RECURSE SOURCES "${SOURCES_DIR}/*.c" "*${SOURCES_DIR}/*.cpp")
file(GLOB_RECURSE HEADER_FILES "${SOURCES_DIR}/*.h" "${SOURCES_DIR}/*.hpp")

# add usearch to compilation target
list(APPEND SOURCES "${CMAKE_SOURCE_DIR}/third_party/usearch/c/lib.cpp")

string(REGEX MATCH "^PostgreSQL (\[0-9]+).*"
  PostgreSQL_VERSION_NUMBER ${PostgreSQL_VERSION_STRING})
set(PG_VERSION ${CMAKE_MATCH_1})

# For Apple and Postgres 16 use .dylib instead of .so
if (APPLE AND PG_VERSION VERSION_GREATER_EQUAL "16")
  set(CMAKE_SHARED_MODULE_SUFFIX ".dylib")
endif()

# ADD LanternDB! Let there be light!
add_library(lantern MODULE ${SOURCES})

# Add postgres extension packaging rules
target_include_directories(
  lantern
  SYSTEM PRIVATE ${PostgreSQL_SERVER_INCLUDE_DIRS}
  PUBLIC ${CMAKE_SOURCE_DIR}/src
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
  )

# include build-generated headers
target_include_directories(lantern PUBLIC ${CMAKE_BINARY_DIR}/include)

# usearch includes
target_include_directories(
  lantern
  PRIVATE "./third_party/usearch/c"
  PRIVATE "./third_party/usearch/include/"
  PRIVATE "./third_party/usearch/fp16/include/"
)

target_compile_options(lantern PRIVATE
    -g
    -Wformat
    -Werror=format-security
    -Wno-unknown-pragmas
    -ftree-vectorize
    -fassociative-math
    -fno-signed-zeros
    -fno-trapping-math
    # -fno-omit-frame-pointer
    -fPIC
)
if(${MARCH_NATIVE})
  target_compile_options(lantern PRIVATE "-march=native")
endif()
# uncomment to debug autovectorization with gcc
# target_compile_options(lantern PRIVATE "-fopt-info-vec")

if(${USEARCH_USE_SIMSIMD})
  target_include_directories(lantern PRIVATE "./third_party/usearch/simsimd/include")
endif()

set(_link_flags "${PostgreSQL_SHARED_LINK_OPTIONS}")
if (${POSTGRES_UBSAN})
  set(_link_flags "-fsanitize=address -Wl,--as-needed -Wl,-rpath,'/usr/local/pgsql/lib',--enable-new-dtags")
  # it seems we should not pass -fsanitize=undefined to the extension we are buildning
  message(INFO "Preparing UBSAN in debug mode link flags: ${_link_flags}")
  # Check if "undefined" is part of the CXXFLAGS
  string(FIND "$ENV{CXXFLAGS}" "undefined" cxx_pos)
  string(FIND "$ENV{CFLAGS}" "undefined" c_pos)
  string(FIND "$ENV{LDFLAGS}" "undefined" ld_pos)
  if(NOT (cxx_pos EQUAL -1 AND c_pos EQUAL -1 AND ld_pos EQUAL -1))
      message(WARNING "Does the CXXFLAGS, CFLAGS or LDFLAGS environment variable contain '--fsanitize=undefined'?
      The flag should not be included in lantern extension build and is only necessary in postgres build")
  endif()
endif()
foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
  set(_link_flags "${_link_flags} -L${_dir}")
endforeach()

if(APPLE)
  set(_link_flags "${_link_flags} -bundle_loader ${PG_BINARY} -undefined dynamic_lookup")
endif()
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # suppress warnings from autovectorization failures such as:
  #   loop not vectorized: the optimizer was unable to perform the
  #   requested transformation; the transformation might be disabled
  #   or specified as part of an unsupported transformation ordering [-Wpass-failed=transform-warning]
  target_compile_options(lantern PRIVATE -Wno-pass-failed)
endif()

set_target_properties(
  lantern
  PROPERTIES PREFIX ""
             LINK_FLAGS "${_link_flags}"
             POSITION_INDEPENDENT_CODE ON)

# THIRD PARTY LIBRARIES

# needed to make sure cmake does not add libstdc++ to the linker command when an
# external cpp library is added more at`
# https://cmake-developers.cmake.narkive.com/JnbrDyGT/setting-linker-language-still-adds-lstdc
if(NOT APPLE)
  # clang handles static libstdc++ differently than gcc
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    find_library(STATIC_LIBSTDCPP NAMES libstdc++.a PATHS ${CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES})

    if(STATIC_LIBSTDCPP)
        set(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "${CMAKE_CXX_IMPLICIT_LINK_LIBRARIES};${STATIC_LIBSTDCPP}")
    endif()
  else()
    # apples does not understand -static-libstdc++ used in usearch to bundle libstdc++ with the
    # created archive.
    # so, on apple we dynamically link to the c++ runtime
    # todo:: find a way to statically link the c++ runtime on mac
    set(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "")
    set(CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES "")
  endif()
endif()

# Note: -static-libstdc++ is only meaningful for dynamic builds.
# If we ever switch back to statically linking usearch or other third party libs,
# we need to explicitly link against libstdc++.a
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_options(lantern PRIVATE -static-libstdc++)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # it seems -Wno-unknown-pragmas is not enough for clang
    target_compile_options(lantern PRIVATE "-Wno-unknown-warning-option")
    target_link_options(lantern PRIVATE -static)
endif ()
# not sure why, but the above is not enough to force pulling cpp runtime into lantern.so
# the following is also necessary
target_link_libraries(lantern PRIVATE "libstdc++.a")
set_target_properties(lantern PROPERTIES LINKER_LANGUAGE C)
# the flag instructs usearch/lib.c to builds with lantern-postgres compatible storage, which
# assumes storage is handled inside postgres and so usearch allocates no memory for it
target_compile_definitions(lantern PRIVATE LANTERN_INSIDE_POSTGRES)
target_compile_definitions(lantern PRIVATE "USEARCH_USE_SIMSIMD=$<BOOL:${USEARCH_USE_SIMSIMD}>")

if (FAILURE_POINTS)
  message(STATUS "Failure points are enabled.")
  target_compile_definitions(lantern PRIVATE LANTERN_FAILURE_POINTS_ARE_ENABLED=1)
else()
  message(STATUS "Failure points are disabled.")
  target_compile_definitions(lantern PRIVATE LANTERN_FAILURE_POINTS_ARE_ENABLED=0)
endif()
if (${LANTERNDB_COPYNODES})
  target_compile_definitions(lantern PRIVATE LANTERNDB_COPYNODES)
endif()

set(_script_file "lantern--${RELEASE_ID}.sql")

# ============== Use clang compiler to emit llvm bytecode =================
find_program(LLVM_LTO NAMES llvm-lto)
if(
  NOT LLVM_LTO STREQUAL "LLVM_LTO-NOTFOUND"
  AND PostgreSQL_WITH_LLVM
  AND CMAKE_C_COMPILER_ID MATCHES "Clang"
  AND CMAKE_CXX_COMPILER_ID MATCHES "Clang"
)
  target_link_options(lantern PRIVATE -flto)
  target_compile_options(lantern PRIVATE  "-emit-llvm")
  add_custom_target(link_llvm_objects ALL
      DEPENDS lantern
      COMMAND ${CMAKE_SOURCE_DIR}/scripts/link_llvm_objects.sh '$<TARGET_OBJECTS:lantern>' ${CMAKE_BINARY_DIR}
  )
  install(DIRECTORY ${CMAKE_BINARY_DIR}/bitcode/ DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR}/bitcode)
  message(STATUS "Using clang compiler to emit llvm bytecode")
endif()
# =========================================================================

set (_update_files
  sql/updates/0.3.0--0.3.1.sql
)

# Generate version information for the binary
EXECUTE_PROCESS(
    COMMAND git log -1 --format=%h
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    OUTPUT_VARIABLE GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
# OPTIONS
set(BUILD_ID "latest-${GIT_HASH}")

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/${_script_file}
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/sql/lantern.sql ${CMAKE_BINARY_DIR}/${_script_file}
  DEPENDS ${CMAKE_SOURCE_DIR}/sql/lantern.sql
  COMMENT "Copying lantern.sql into a versioned filename"
)

add_custom_target(phony_always_runs ALL DEPENDS ${CMAKE_BINARY_DIR}/${_script_file})

set(VERSION_HEADER_TEMPLATE "${CMAKE_MODULE_PATH}/version.h.template")
set(VERSION_HEADER_OUTPUT "${CMAKE_BINARY_DIR}/include/version.h")
configure_file(${VERSION_HEADER_TEMPLATE} ${VERSION_HEADER_OUTPUT})

# AUTO-GENERATE lantern.control file for PostgreSQL
set(CONTROL_TEMPLATE "${CMAKE_MODULE_PATH}/lantern.control.template")
set(CONTROL_OUTPUT "${CMAKE_BINARY_DIR}/lantern.control")
configure_file(${CONTROL_TEMPLATE} ${CONTROL_OUTPUT})

install(TARGETS lantern LIBRARY DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
install(FILES ${CONTROL_OUTPUT} ${CMAKE_BINARY_DIR}/${_script_file}
        DESTINATION ${PostgreSQL_EXTENSION_DIR})

foreach(_update_file ${_update_files})
  get_filename_component(_update_file_name ${_update_file} NAME)
  install(FILES ${_update_file} DESTINATION ${PostgreSQL_EXTENSION_DIR} RENAME "lantern--${_update_file_name}")
endforeach()

# UNINSTALL
add_custom_target(
  uninstall
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_EXTENSION_DIR}/${CONTROL_OUTPUT}
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_EXTENSION_DIR}/${_script_file}
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_PACKAGE_LIBRARY_DIR}/lantern.so)

# TEST
add_custom_target(
  test
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_all_tests.sh --regression
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test
)

add_custom_target(
  test-parallel
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_all_tests.sh --parallel
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test
)

# measure code coverage after running make test
add_custom_target(cover
    COMMAND lcov --rc lcov_branch_coverage=1 --directory . --capture --output-file coverage.info
    COMMAND lcov --rc lcov_branch_coverage=1 --remove coverage.info '/usr/*' --output-file coverage.info
    COMMAND lcov --rc lcov_branch_coverage=1 --remove coverage.info '*/test/c/*' --output-file coverage.info
    COMMAND lcov --rc lcov_branch_coverage=1 --list coverage.info
    COMMAND genhtml coverage.info --branch-coverage --rc genhtml_branch_coverage=1 --output-directory out
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

add_custom_target(
  test-misc
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_all_tests.sh --misc
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test
)

# BENCHMARK
add_custom_target(
  benchmark
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_benchmarks.sh
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/build
)
add_custom_target(
  benchmark-skip-setup
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_benchmarks.sh --skip-setup
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/build
)
add_custom_target(
  benchmark-print-only
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_benchmarks.sh --print-only
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/build
)

# DEVELOPMENT
find_program(CLANG_FORMAT NAMES clang-format)

if (CLANG_FORMAT)
  execute_process(COMMAND ${CLANG_FORMAT} --version
  OUTPUT_VARIABLE CLANG_FORMAT_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE)

  string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" CLANG_FORMAT_VERSION "${CLANG_FORMAT_VERSION}")

  if(CLANG_FORMAT_VERSION VERSION_LESS 13)
    message(WARNING "clang-format version ${CLANG_FORMAT_VERSION} found, need at least 13")
    set(CLANG_FORMAT OFF)
  endif()
endif()


if(DEV AND NOT CLANG_FORMAT)
  message(FATAL_ERROR "clang-format not found!")
endif()

if (CLANG_FORMAT)
  set(CLANG_FORMATTABLE_FILES ${SOURCES})
  list(APPEND CLANG_FORMATTABLE_FILES ${HEADER_FILES})

  # Add format target
  add_custom_target(
    format
    COMMAND ${CLANG_FORMAT} -i ${CLANG_FORMATTABLE_FILES}
    COMMENT "Formatting code with clang-format"
    VERBATIM
  )
  # Add format check target
  add_custom_target(
    format_check
    COMMAND ${CLANG_FORMAT} --dry-run -Werror ${CLANG_FORMATTABLE_FILES}
    COMMENT "Checking code formatting with clang-format"
    VERBATIM
  )
endif()

# Package universal install script
add_custom_target(
  archive
  ${CMAKE_COMMAND} -E env SOURCE_DIR=${CMAKE_SOURCE_DIR} BUILD_DIR=${CMAKE_BINARY_DIR} PG_VERSION=${PG_VERSION} ${CMAKE_SOURCE_DIR}/scripts/package.sh
  DEPENDS ${CMAKE_BINARY_DIR}/${_script_file}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
add_dependencies(archive lantern)

# Debian packaging
set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "postgresql-${PG_VERSION}")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Lantern Data")
include(CPack)

