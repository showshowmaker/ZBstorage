cmake_minimum_required(VERSION 3.16)
project(SRM_MAIN LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(GFlags REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(BRPC REQUIRED brpc)

if(NOT TARGET storagenode_proto)
  add_subdirectory(${CMAKE_SOURCE_DIR}/src/msg/proto ${CMAKE_BINARY_DIR}/src/msg/proto_build)
endif()

set(SRM_MAIN_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ClusterManagerServiceImpl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/StorageNodeManager.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/NodeRegistry.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/gateway/GatewayServiceImpl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/gateway/RequestDispatcher.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/simulation/VirtualNodeEngine.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/virtual/VirtualNodeLedger.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/virtual/InodeBatchMonitor.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/virtual/VirtualNodeController.cpp
  ${CMAKE_SOURCE_DIR}/common/StatusUtils.cpp
  ${CMAKE_SOURCE_DIR}/fs/volume/Volume.cpp
  ${CMAKE_SOURCE_DIR}/fs/block/BlockManager.cpp
  ${CMAKE_SOURCE_DIR}/fs/volume/StorageResourceGlobal.cpp
  ${CMAKE_SOURCE_DIR}/fs/volume/VolumeManager.cpp
  ${CMAKE_SOURCE_DIR}/fs/volume/VolumeRegistry.cpp
  ${CMAKE_SOURCE_DIR}/srm/storage_manager/StorageResource.cpp
  ${CMAKE_SOURCE_DIR}/mds/inode/inode.cpp
  ${CMAKE_SOURCE_DIR}/mds/inode/InodeTimestamp.cpp
  ${CMAKE_SOURCE_DIR}/debug/ZBLog.cpp
)

add_executable(srm_main ${SRM_MAIN_SOURCES})

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

target_include_directories(srm_main
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/common
    ${CMAKE_SOURCE_DIR}/fs
    ${CMAKE_SOURCE_DIR}/fs/block
    ${CMAKE_SOURCE_DIR}/fs/volume
    ${CMAKE_SOURCE_DIR}/srm
    ${CMAKE_SOURCE_DIR}/srm/storage_manager
    ${CMAKE_SOURCE_DIR}/debug
    ${CMAKE_BINARY_DIR}/msg/RPC
    ${CMAKE_BINARY_DIR}/msg
    ${CMAKE_BINARY_DIR}/msg/proto
    ${BRPC_INCLUDE_DIRS}
)

target_link_libraries(srm_main
  PRIVATE
    storagenode_proto
    rpc_proto
    zb_storagenode
    mds_server
    ${_BRPC_LINK}
    ${_GFLAGS_LINK}
)

set_target_properties(srm_main PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
)
