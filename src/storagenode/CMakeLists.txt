# CMake for storagenode module
# This subdir builds a reusable static library containing storage node & device implementations.

# Do NOT call project() here to keep root configuration untouched.

# Optional dependency: nlohmann_json (header-only)
# Try to use a system/packaged version first; fallback to FetchContent if not found.
find_package(nlohmann_json 3.2.0 QUIET)
if(NOT nlohmann_json_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  )
  FetchContent_MakeAvailable(nlohmann_json)
endif()

add_library(zb_storagenode STATIC
  StoreageNode.cpp
  hard_disc/HDD.cpp
  hard_disc/SSD.cpp
  optical/OpticalDisc.cpp
  optical/OpticalDiscLibrary.cpp
)

target_include_directories(zb_storagenode
  PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/storagenode
    ${CMAKE_SOURCE_DIR}/src/storagenode/hard_disc
    ${CMAKE_SOURCE_DIR}/src/storagenode/optical
    ${CMAKE_SOURCE_DIR}/src/fs
    ${CMAKE_SOURCE_DIR}/src/fs/block
    ${CMAKE_SOURCE_DIR}/src/fs/volume
)

target_link_libraries(zb_storagenode
  PUBLIC
    nlohmann_json::nlohmann_json
)

set_target_properties(zb_storagenode PROPERTIES
  OUTPUT_NAME zb_storagenode
  POSITION_INDEPENDENT_CODE ON
)

# Consumers should additionally link Volume/Block libraries when creating executables that
# instantiate or call into Volume defined in src/fs/volume/Volume.cpp. Keeping this library
# STATIC avoids link-time resolution issues at archive creation.

# Real node brpc server/client targets.
add_subdirectory(real_node)
