cmake_minimum_required(VERSION 3.10)
project(zb_system_demo CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)
find_package(GFlags REQUIRED)
find_package(Threads REQUIRED)

pkg_check_modules(BRPC REQUIRED brpc)
pkg_check_modules(FUSE REQUIRED fuse)

set(PROJECT_ROOT ${CMAKE_CURRENT_LIST_DIR}/..)

add_executable(zb_system_test
  zb_system_test.cpp
  ${PROJECT_ROOT}/src/client/mount/DfsClient.cpp
  ${PROJECT_ROOT}/src/client/mount/RpcClients.cpp
  ${PROJECT_ROOT}/src/common/StatusUtils.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeStorage.cpp
  ${PROJECT_ROOT}/src/mds/inode/inode.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeTimestamp.cpp
)

target_include_directories(zb_system_test PRIVATE
  ${PROJECT_ROOT}/src
  ${PROJECT_ROOT}/src/client
  ${PROJECT_ROOT}/src/client/mount
  ${PROJECT_ROOT}/src/common
  ${PROJECT_ROOT}/src/mds
  ${PROJECT_ROOT}/src/debug
  ${PROJECT_ROOT}/third_party
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/msg/RPC
  ${CMAKE_BINARY_DIR}/msg/RPC/proto
  ${CMAKE_BINARY_DIR}/msg
  ${CMAKE_BINARY_DIR}/msg/proto
  ${FUSE_INCLUDE_DIRS}
)

target_link_libraries(zb_system_test PRIVATE
  rpc_proto
  storagenode_proto
  ${Protobuf_LIBRARIES}
  ${BRPC_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${FUSE_LIBRARIES}
  Threads::Threads
)
