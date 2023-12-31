cmake_minimum_required(VERSION 3.3)

set(LANTERNDB_VERSION 0.0.11)

project(
  LanternDB
  VERSION ${LANTERNDB_VERSION}
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

# OPTIONS
option(DEV "Developer mode: provide code formatting, get postgres source, etc." OFF)
option(BUILD_WITH_USEARCH "Build with usearch as hnsw provider" ON)
option(BUILD_LIBHNSW "Build libhnsw as hnsw provider" OFF)
option(CODECOVERAGE "Enable code coverage for the build" OFF)
option(BENCH "Enable benchmarking" OFF)
option(FAILURE_POINTS "Enable failure points" ON)
option(BUILD_C_TESTS "Build C client tests" OFF)

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

if (BENCH)
  message(STATUS "Benchmarking is enabled.")
  add_compile_definitions(LANTERN_BENCH)
endif()

# options passed into lantern sourcecode
# todo:: tests for copynodes=ON are broken
option(LANTERNDB_COPYNODES "Copy postgres index tuples for external retriever during scan instead of pinning" OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

message(STATUS "${CMAKE_COLOR_GREEN}Build type: ${CMAKE_BUILD_TYPE}${CMAKE_COLOR_RESET}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wno-conversion -Wno-unknown-pragmas")

find_package(PostgreSQL REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SOURCES_DIR "src")
# todo:: list out all the files instead of glob
file(GLOB SOURCES "${SOURCES_DIR}/**/*.c" "${SOURCES_DIR}/*.c")
file(GLOB HEADER_FILES "${SOURCES_DIR}/**/*.h" "${SOURCES_DIR}/*.h")

set(USEARCH_USE_SIMSIMD OFF)
set(USEARCH_LOOKUP_LABEL OFF)
# we always use a usearch index from a single thread
set(USEARCH_CONCURRENT OFF)
set(USEARCH_BUILD_STATIC ON)
# do not add asan flags to usearch debug build of the library since
# we still have not figured out how to load those into postgres
set(USEARCH_DEBUG_BUILD_ASAN OFF)
# the var below can be used to skip -march=native in the usearch build
# which causes issues when built from docker on m1 macs
# set(USEARCH_NO_MARCH_NATIVE OFF)
set(USEARCH_BUILD_TEST OFF)
set(USEARCH_BUILD_BENCHMARK OFF)
add_subdirectory("./third_party/usearch/c")

string(REGEX MATCH "^PostgreSQL (\[0-9]+).*"
  PostgreSQL_VERSION_NUMBER ${PostgreSQL_VERSION_STRING})
set(PG_VERSION ${CMAKE_MATCH_1})

# For Apple and Postgres 16 use .dylib instead of .so
if (APPLE AND PG_VERSION VERSION_GREATER_EQUAL "16")
  set(CMAKE_SHARED_MODULE_SUFFIX ".dylib")
endif()

# ADD LanternDB! Let there be light!
add_library(lantern MODULE ${SOURCES})

# ============= Lantern tests with C libpq ==========
if (BUILD_C_TESTS)
  add_executable(lantern_c_tests "test/c/runner.c")
  # Add postgres header include dirs
  target_include_directories(
    lantern_c_tests
    SYSTEM PRIVATE ${PostgreSQL_INCLUDE_DIRS})
  # Add link directories for postgres shared libraries
  target_link_directories(lantern_c_tests PRIVATE ${PostgreSQL_LIBRARY_DIRS})
  # Link libpq
  target_link_libraries(lantern_c_tests "-lpq")
  
  add_custom_target(
    test-client
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_all_tests.sh --client
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/
  )
  add_dependencies(test-client lantern_c_tests)
endif()
# ==================================================

# Add postgres extension packaging rules
target_include_directories(
  lantern
  SYSTEM PRIVATE ${PostgreSQL_SERVER_INCLUDE_DIRS}
  PUBLIC ${CMAKE_SOURCE_DIR}/src
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set(_link_flags "${PostgreSQL_SHARED_LINK_OPTIONS}")
foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
  set(_link_flags "${_link_flags} -L${_dir}")
endforeach()

if(APPLE)
  set(_link_flags "${_link_flags} -bundle_loader ${PG_BINARY} -undefined dynamic_lookup")
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

set_target_properties(lantern PROPERTIES LINKER_LANGUAGE C)

target_include_directories(lantern PRIVATE "./third_party/usearch/c")
target_link_directories(lantern PRIVATE "./src")
if (${BUILD_WITH_USEARCH})
  target_link_libraries(lantern PRIVATE usearch_c)
  target_compile_definitions(lantern PRIVATE LANTERN_USE_USEARCH)
endif()
if (${BUILD_WITH_LIBHNSW})
  target_link_libraries(lantern PRIVATE hnsw)
  target_compile_definitions(lantern PRIVATE LANTERN_USE_LIBHNSW)
endif()
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

set(_script_file "lantern--${LANTERNDB_VERSION}.sql")
set (_update_files
  sql/updates/0.0.4--0.0.5.sql
  sql/updates/0.0.5--0.0.6.sql
  sql/updates/0.0.6--0.0.7.sql
  sql/updates/0.0.7--0.0.8.sql
  sql/updates/0.0.8--0.0.9.sql
  sql/updates/0.0.9--0.0.10.sql
  sql/updates/0.0.10--0.0.11.sql
  sql/updates/0.0.11--0.0.12.sql
)

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

target_include_directories(lantern PUBLIC ${CMAKE_BINARY_DIR}/include)


# AUTO-GENERATE lantern.control file for PostgreSQL
set(CONTROL_TEMPLATE "${CMAKE_MODULE_PATH}/lantern.control.template")
set(CONTROL_OUTPUT "${CMAKE_BINARY_DIR}/lantern.control")
configure_file(${CONTROL_TEMPLATE} ${CONTROL_OUTPUT})


# INSTALL

install(TARGETS lantern LIBRARY DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
install(FILES ${CONTROL_OUTPUT} ${CMAKE_BINARY_DIR}/${_script_file}
        DESTINATION ${PostgreSQL_EXTENSION_DIR})

foreach(_update_file ${_update_files})
  get_filename_component(_update_file_name ${_update_file} NAME)
  install(FILES ${_update_file} DESTINATION ${PostgreSQL_EXTENSION_DIR} RENAME "lantern--${_update_file_name}")
endforeach()

# todo:: add llvm bytecode generation for postgres optimizations (see how the
# canonical extension building infra extension does it)

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

  if(CLANG_FORMAT_VERSION VERSION_LESS 14)
    message(WARNING "clang-format version ${CLANG_FORMAT_VERSION} found, need at least 14")
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
set(CPACK_DEBIAN_PACKAGE_DEPENDS "postgresql-${PG_VERSION}, postgresql-${PG_VERSION}-pgvector")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Lantern Data")
include(CPack)

