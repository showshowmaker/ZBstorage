cmake_minimum_required(VERSION 3.10)
project(ZBStorage_rpc_servers LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# project root (父目录)，保持和原来的 CMake 写法一致
set(PROJECT_ROOT "${CMAKE_CURRENT_LIST_DIR}/..")

# Option to enable zbss debug logging for targets.
option(ENABLE_ZBSS_LOG "Compile servers with ZBSS_ENABLE_LOG (emit debug expectation logs)" OFF)
set(DEFAULT_ZBSS_LOG_LEVEL 2 CACHE STRING "Default ZBSS log level when ENABLE_ZBSS_LOG is ON (0=ERROR..3=DEBUG)")

# 公共源码，直接复用原来 test CMake 里的 COMMON_SRCS 列表
# （如果后面想精简每个 server 的依赖，可以再按需拆分）
set(COMMON_SRCS
  ${PROJECT_ROOT}/src/storagenode/optical/OpticalDiscLibrary.cpp
  ${PROJECT_ROOT}/src/storagenode/optical/OpticalDisc.cpp
  ${PROJECT_ROOT}/src/storagenode/StoreageNode.cpp
  ${PROJECT_ROOT}/src/storagenode/hard_disc/SSD.cpp
  ${PROJECT_ROOT}/src/storagenode/hard_disc/HDD.cpp
  ${PROJECT_ROOT}/src/fs/block/BlockManager.cpp
  ${PROJECT_ROOT}/src/fs/volume/Volume.cpp
  ${PROJECT_ROOT}/src/fs/volume/StorageResourceGlobal.cpp
  ${PROJECT_ROOT}/src/fs/volume/VolumeManager.cpp
  ${PROJECT_ROOT}/src/fs/volume/VolumeRegistry.cpp
  ${PROJECT_ROOT}/src/fs/VFS/VFS_new.cpp
  ${PROJECT_ROOT}/src/fs/io/LocalStorageGateway.cpp
  ${PROJECT_ROOT}/src/srm/storage_manager/StorageResource.cpp
  ${PROJECT_ROOT}/src/mds/server/Server.cpp
  ${PROJECT_ROOT}/src/mds/server/DirectoryLockTable.cpp
  ${PROJECT_ROOT}/src/mds/server/DirStore.cpp
  ${PROJECT_ROOT}/src/mds/allocator/VolumeAllocator.cpp
  ${PROJECT_ROOT}/src/mds/metadataserver/MetadataManager.cpp
  ${PROJECT_ROOT}/src/mds/metadataserver/KVStore.cpp
  ${PROJECT_ROOT}/src/mds/collector/collector.cpp
  ${PROJECT_ROOT}/src/srm/image_manager/ImageManager.cpp
  ${PROJECT_ROOT}/src/mds/inode/inode.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeStorage.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeTimestamp.cpp
  ${PROJECT_ROOT}/src/debug/ZBLog.cpp
)

# proto 相关路径
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(VFS_PROTO_SRCS     ${PROTO_DIR}/vfs.pb.cc)
set(MDS_PROTO_SRCS     ${PROTO_DIR}/mds.pb.cc)
set(STORAGE_PROTO_SRCS ${PROTO_DIR}/storage.pb.cc)

# 一个公共函数，给三个 server 统一设置 include/编译选项/日志宏等
function(setup_server_target target_name)
  target_include_directories(${target_name} PRIVATE
    ${PROJECT_ROOT}/src
    ${PROJECT_ROOT}/src/storagenode
    ${PROJECT_ROOT}/src/storagenode/hard_disc
    ${PROJECT_ROOT}/src/fs
    ${PROJECT_ROOT}/src/mds
    ${PROJECT_ROOT}/src/debug
    ${PROJECT_ROOT}/src/srm
    ${PROJECT_ROOT}/third_party
    ${PROTO_DIR}
  )

  target_compile_options(${target_name} PRIVATE
    -Wall -Wextra -Wno-unused-parameter
  )

  if(ENABLE_ZBSS_LOG)
    target_compile_definitions(${target_name} PRIVATE ZBSS_ENABLE_LOG)
    target_compile_definitions(${target_name} PRIVATE ZBSS_DEFAULT_LOG_LEVEL=${DEFAULT_ZBSS_LOG_LEVEL})
  endif()

  # 如果你在上层 CMake 里已经通过 find_package 或 add_subdirectory
  # 定义了 brpc/protobuf/gflags/glog 等 target，可以在这里统一链接，例如：
  #
  # target_link_libraries(${target_name} PRIVATE
  #   brpc
  #   protobuf
  #   gflags
  #   glog
  #   pthread
  # )
  #
  # 具体库名可以根据你现有工程的设置调整。
endfunction()

# 1. VFS server
add_executable(vfs_server
  ${CMAKE_CURRENT_SOURCE_DIR}/vfs_server.cpp
  ${VFS_PROTO_SRCS}
  ${COMMON_SRCS}
)
setup_server_target(vfs_server)

# 2. MDS server
add_executable(mds_server
  ${CMAKE_CURRENT_SOURCE_DIR}/mds_server.cpp
  ${MDS_PROTO_SRCS}
  ${COMMON_SRCS}
)
setup_server_target(mds_server)

# 3. Storage server
add_executable(storage_server
  ${CMAKE_CURRENT_SOURCE_DIR}/storage_server.cpp
  ${STORAGE_PROTO_SRCS}
  ${COMMON_SRCS}
)
setup_server_target(storage_server)

# 可选：一个聚合目标，一次性编译三个 server
add_custom_target(build_rpc_servers
  DEPENDS vfs_server mds_server storage_server
)
