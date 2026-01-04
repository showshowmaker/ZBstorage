cmake_minimum_required(VERSION 3.10)
project(dfs_fuse_client CXX)

find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse)
find_package(Protobuf REQUIRED)
find_package(GFlags REQUIRED)
pkg_check_modules(BRPC REQUIRED brpc)

include_directories(
  ${FUSE_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/src/common
  ${CMAKE_SOURCE_DIR}/src/client
  ${CMAKE_BINARY_DIR}/src/msg/RPC
  ${CMAKE_BINARY_DIR}/src/msg
)

# New mount client
add_executable(zb_fuse_client
  zb_fuse_main.cpp
  ../mount/DfsClient.cpp
  ../mount/RpcClients.cpp
  ${CMAKE_SOURCE_DIR}/src/common/StatusUtils.cpp
)
target_compile_definitions(zb_fuse_client PRIVATE _FILE_OFFSET_BITS=64)
target_link_libraries(zb_fuse_client
  PRIVATE
    rpc_proto
    ${Protobuf_LIBRARIES}
    ${BRPC_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${FUSE_LIBRARIES}
    ${CMAKE_CXX_STANDARD_LIBRARIES}
)
