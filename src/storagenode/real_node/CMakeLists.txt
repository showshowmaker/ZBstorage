cmake_minimum_required(VERSION 3.10)

find_package(Protobuf REQUIRED)
find_package(GFlags REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(BRPC REQUIRED brpc)

# Ensure proto target is available (expected from top-level add_subdirectory).
if(NOT TARGET storagenode_proto)
  add_subdirectory(${CMAKE_SOURCE_DIR}/src/msg/proto ${CMAKE_BINARY_DIR}/src/msg/proto_build)
endif()

add_executable(real_node_server
  server/real_node_server.cpp
  server/StorageServiceImpl.cpp
  io/DiskManager.cpp
  io/IOEngine.cpp
)

add_executable(real_node_client
  test/real_node_client.cpp
)

# Prefer modern brpc target when available, fall back to libraries list.
set(_BRPC_LINK brpc::brpc)
if(NOT TARGET brpc::brpc)
  set(_BRPC_LINK ${BRPC_LIBRARIES})
endif()

if(TARGET gflags::gflags)
  set(_GFLAGS_LINK gflags::gflags)
elseif(GFLAGS_LIBRARIES)
  set(_GFLAGS_LINK ${GFLAGS_LIBRARIES})
elseif(GFLAGS_LIBRARY)
  set(_GFLAGS_LINK ${GFLAGS_LIBRARY})
else()
  set(_GFLAGS_LINK gflags)
endif()

target_include_directories(real_node_server
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${BRPC_INCLUDE_DIRS}
)

target_include_directories(real_node_client
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${BRPC_INCLUDE_DIRS}
)

target_link_libraries(real_node_server
  PRIVATE
    storagenode_proto
    ${_BRPC_LINK}
    ${_GFLAGS_LINK}
)

target_link_libraries(real_node_client
  PRIVATE
    storagenode_proto
    ${_BRPC_LINK}
    ${_GFLAGS_LINK}
)

set_target_properties(real_node_server real_node_client PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
)
