cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME sistat)

set(SISTAT_WASM_BUILD OFF)
if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    set(SISTAT_WASM_BUILD ON)
else()
    find_package(OpenSSL REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
# Include the local copy of httplib (required for sistat)
include_directories(third_party/httplib)

# Define sources: Main entry point + library logic
set(EXTENSION_SOURCES src/sistat_extension.cpp)
# This includes files defined in src/sistat/CMakeLists.txt
if(NOT SISTAT_WASM_BUILD)
    add_subdirectory(src/sistat)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(NOT SISTAT_WASM_BUILD)
    target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
