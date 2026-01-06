cmake_minimum_required(VERSION 3.10)
project(zb_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# brpc assumed installed system-wide
find_library(BRPC_LIB brpc)
if(NOT BRPC_LIB)
  message(FATAL_ERROR "brpc library not found. Please install brpc or set BRPC_LIB.")
endif()
find_library(GFLAGS_LIB gflags)
if(NOT GFLAGS_LIB)
  message(FATAL_ERROR "gflags library not found. Please install gflags.")
endif()

set(PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_common.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/mds.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/storage.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/vfs.proto
)

set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Repository root where source lives.
set(REPO_ROOT ${CMAKE_SOURCE_DIR})

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/proto
  ${CMAKE_CURRENT_SOURCE_DIR}/server
  ${CMAKE_CURRENT_SOURCE_DIR}/client
  ${REPO_ROOT}
  ${REPO_ROOT}/storagenode
  ${REPO_ROOT}/storagenode/hard_disc
  ${REPO_ROOT}/fs
  ${REPO_ROOT}/mds
  ${REPO_ROOT}/debug
  ${REPO_ROOT}/srm
  ${REPO_ROOT}/third_party
  ${CMAKE_CURRENT_BINARY_DIR}  # for generated *.pb.h
)

set(COMMON_SRCS
  ${REPO_ROOT}/common/StatusUtils.cpp
  ${REPO_ROOT}/storagenode/optical/OpticalDiscLibrary.cpp
  ${REPO_ROOT}/storagenode/optical/OpticalDisc.cpp
  ${REPO_ROOT}/storagenode/StoreageNode.cpp
  ${REPO_ROOT}/storagenode/hard_disc/SSD.cpp
  ${REPO_ROOT}/storagenode/hard_disc/HDD.cpp
  ${REPO_ROOT}/fs/block/BlockManager.cpp
  ${REPO_ROOT}/fs/volume/Volume.cpp
  ${REPO_ROOT}/fs/volume/StorageResourceGlobal.cpp
  ${REPO_ROOT}/fs/volume/VolumeManager.cpp
  ${REPO_ROOT}/fs/volume/VolumeRegistry.cpp
  ${REPO_ROOT}/fs/VFS/VFS_new.cpp
  ${REPO_ROOT}/fs/io/LocalStorageGateway.cpp
  ${REPO_ROOT}/srm/storage_manager/StorageResource.cpp
  ${REPO_ROOT}/mds/server/Server.cpp
  ${REPO_ROOT}/mds/server/DirectoryLockTable.cpp
  ${REPO_ROOT}/mds/server/DirStore.cpp
  ${REPO_ROOT}/mds/allocator/VolumeAllocator.cpp
  ${REPO_ROOT}/mds/metadataserver/MetadataManager.cpp
  ${REPO_ROOT}/mds/metadataserver/KVStore.cpp
  ${REPO_ROOT}/mds/collector/collector.cpp
  ${REPO_ROOT}/srm/image_manager/ImageManager.cpp
  ${REPO_ROOT}/mds/inode/inode.cpp
  ${REPO_ROOT}/mds/inode/InodeStorage.cpp
  ${REPO_ROOT}/mds/inode/InodeTimestamp.cpp
  ${REPO_ROOT}/debug/ZBLog.cpp
)

add_library(rpc_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(rpc_proto PUBLIC ${Protobuf_LIBRARIES})

add_executable(mds_rpc_server
  server/mds_service_impl.cpp
  ${COMMON_SRCS}
)
target_link_libraries(mds_rpc_server PRIVATE rpc_proto ${Protobuf_LIBRARIES} ${BRPC_LIB} ${GFLAGS_LIB})

add_executable(storage_rpc_server
  server/storage_service_impl.cpp
  ${COMMON_SRCS}
)
target_link_libraries(storage_rpc_server PRIVATE rpc_proto ${Protobuf_LIBRARIES} ${BRPC_LIB} ${GFLAGS_LIB})

add_executable(vfs_rpc_server
  server/vfs_service_impl.cpp
  ${COMMON_SRCS}
)
target_link_libraries(vfs_rpc_server PRIVATE rpc_proto ${Protobuf_LIBRARIES} ${BRPC_LIB} ${GFLAGS_LIB})

add_executable(rpc_client
  client/VFS_dist.cpp
  ${COMMON_SRCS}
)
target_link_libraries(rpc_client PRIVATE rpc_proto ${Protobuf_LIBRARIES} ${BRPC_LIB} ${GFLAGS_LIB})
