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

# Legacy C-wrapper client
add_executable(dfs_fuse_client fuse_main.cpp)
target_compile_definitions(dfs_fuse_client PRIVATE 
    _FILE_OFFSET_BITS=64
    FUSE_DEBUG 
)
target_link_libraries(dfs_fuse_client
    PRIVATE
        dfs_client
        ${FUSE_LIBRARIES}
        ${CMAKE_CXX_STANDARD_LIBRARIES}
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
