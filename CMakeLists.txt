cmake_minimum_required(VERSION 3.3)

set(LANTERNDB_VERSION 0.0.2)

project(
  LanternDB
  VERSION ${LANTERNDB_VERSION}
  LANGUAGES C CXX)

if(POLICY CMP0077)
  # Allow parent project to override options of children obtain via FetchContent
  # or add_subdirectory.
  cmake_policy(SET CMP0077 NEW)
endif()

# OPTIONS
option(DEV "Developer mode: provide code formatting, get postgres source, etc." OFF)
option(BUILD_WITH_USEARCH "Build with usearch as hnsw provider" ON)
option(BUILD_LIBHNSW "Build libhnsw as hnsw provider" OFF)

# options passed into lanterndb sourcecode
option(LANTERNDB_COPYNODES "Copy postgres index tuples for external retriever instead of rlocking" ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE "Release")
message(STATUS "${CMAKE_COLOR_GREEN}Build type: ${CMAKE_BUILD_TYPE}${CMAKE_COLOR_RESET}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

find_package(PostgreSQL REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SOURCES_DIR "src")
# todo:: list out all the files instead of glob
file(GLOB SOURCES "${SOURCES_DIR}/**/*.c" "${SOURCES_DIR}/*.c")

set(USEARCH_USE_SIMSIMD OFF)
set(USEARCH_BUILD_LIBUSEARCH_STATIC ON)
set(USEARCH_BUILD_LIBUSEARCH_SHARED OFF)
# the var below can be used to skip -march=native in the usearch build
# which causes issues when built from docker on m1 macs
# set(USEARCH_NO_MARCH_NATIVE OFF)
set(USEARCH_BUILD_TEST OFF)
set(USEARCH_BUILD_BENCHMARK OFF)
add_subdirectory("./third_party/usearch/c")

# ADD LanternDB! Let there be light!
add_library(lanterndb MODULE ${SOURCES})

# Add postgres extension packaging rules
target_include_directories(
  lanterndb
  PRIVATE ${PostgreSQL_SERVER_INCLUDE_DIRS}
  PUBLIC ${CMAKE_SOURCE_DIR}/src
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set(_link_flags "${PostgreSQL_SHARED_LINK_OPTIONS}")
foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
  set(_link_flags "${_link_flags} -L${_dir}")
endforeach()

if(APPLE)
  set(_link_flags "${_link_flags} -bundle_loader ${PG_BINARY}")
endif()

set_target_properties(
  lanterndb
  PROPERTIES PREFIX ""
             LINK_FLAGS "${_link_flags}"
             POSITION_INDEPENDENT_CODE ON)

# THIRD PARTY LIBRARIES

# needed to make sure cmake does not add libstdc++ to the linker command when an
# external cpp library is added more at`
# https://cmake-developers.cmake.narkive.com/JnbrDyGT/setting-linker-language-still-adds-lstdc

if(NOT APPLE)
  # apples does not understand -static-libstdc++ used in usearch to bundle libstdc++ with the
  # created archive.
  # so, on apple we dynamically link to the c++ runtime
  # todo:: find a way to statically link the c++ runtime on mac
  set(CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "")
  set(CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES "")
endif()
set_target_properties(lanterndb PROPERTIES LINKER_LANGUAGE C)

target_include_directories(lanterndb PRIVATE "./third_party/usearch/c")
target_link_directories(lanterndb PRIVATE "./src")
if (${BUILD_WITH_USEARCH})
  target_link_libraries(lanterndb PRIVATE usearch_c)
  target_compile_definitions(lanterndb PRIVATE LANTERN_USE_USEARCH)
endif()
if (${BUILD_WITH_LIBHNSW})
  target_link_libraries(lanterndb PRIVATE hnsw)
  target_compile_definitions(lanterndb PRIVATE LANTERN_USE_LIBHNSW)
endif()
if (${LANTERNDB_COPYNODES})
  target_compile_definitions(lanterndb PRIVATE LANTERNDB_COPYNODES)
endif()

target_compile_definitions(lanterndb PRIVATE _GNU_SOURCE)

set(_script_file "lanterndb--${LANTERNDB_VERSION}.sql")
set (_update_files
  sql/updates/0.0.1--0.0.2.sql)

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/${_script_file}
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/sql/lanterndb.sql ${CMAKE_BINARY_DIR}/${_script_file}
  DEPENDS ${CMAKE_SOURCE_DIR}/sql/lanterndb.sql
  COMMENT "Copying lanterndb.sql into a versioned filename"
)

add_custom_target(phony_always_runs ALL DEPENDS ${CMAKE_BINARY_DIR}/${_script_file})

# todo:: make this generated
set(_control_file "lanterndb.control")

# INSTALL

install(TARGETS lanterndb LIBRARY DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
install(FILES ${_control_file} ${CMAKE_BINARY_DIR}/${_script_file}
        DESTINATION ${PostgreSQL_EXTENSION_DIR})

foreach(_update_file ${_update_files})
  get_filename_component(_update_file_name ${_update_file} NAME)
  install(FILES ${_update_file} DESTINATION ${PostgreSQL_EXTENSION_DIR} RENAME "lanterndb--${_update_file_name}")
endforeach()

# todo:: add llvm bytecode generation for postgres optimizations (see how the
# canonical extension building infra extension does it)

# UNINSTALL
add_custom_target(
  uninstall
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_EXTENSION_DIR}/${_control_file}
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_EXTENSION_DIR}/${_script_file}
  COMMAND ${CMAKE_COMMAND} -E remove -f
          ${PostgreSQL_PACKAGE_LIBRARY_DIR}/lanterndb.so)

# TEST
add_custom_target(
  test
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/run_all_tests.sh
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
 )

 # DEVELOPMENT
find_program(CLANG_FORMAT NAMES clang-format)
if(DEV AND NOT CLANG_FORMAT)
  message(FATAL_ERROR "clang-format not found!")
endif()

if (CLANG_FORMAT)
  # Add format target
  add_custom_target(
    format
    COMMAND ${CLANG_FORMAT} -i ${SOURCES}
    COMMENT "Formatting code with clang-format"
    VERBATIM
  )
endif()

add_custom_target(
  bench
  COMMAND ${CMAKE_SOURCE_DIR}/scripts/bench.sh
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
 )