cmake_minimum_required(VERSION 3.3)
project(LanternDB)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(PostgreSQL REQUIRED)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/
        DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR}
        FILES_MATCHING
        PATTERN "*.so")

install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/
        DESTINATION ${PostgreSQL_EXTENSION_DIR}
        FILES_MATCHING
        PATTERN "*.control"
        PATTERN "*.sql")
