cmake_minimum_required(VERSION 3.10)
project(ZBStorage_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

# Gather all .cpp test sources in this directory and subdirectories
file(GLOB_RECURSE TEST_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Optional toggle for fs_verify
option(ENABLE_FS_VERIFY "Build fs_verify test" ON)
if(NOT ENABLE_FS_VERIFY)
  list(FILTER TEST_SRCS EXCLUDE REGEX ".*/fs_verify\\.cpp$")
endif()

# project root (父目录)
set(PROJECT_ROOT "${CMAKE_CURRENT_LIST_DIR}/..")
# Option to enable zbss debug logging for test targets.
option(ENABLE_ZBSS_LOG "Compile tests with ZBSS_ENABLE_LOG (emit debug expectation logs)" OFF)
set(DEFAULT_ZBSS_LOG_LEVEL 2 CACHE STRING "Default ZBSS log level when ENABLE_ZBSS_LOG is ON (0=ERROR..3=DEBUG)")
if(TEST_SRCS)
  message(STATUS "Found test sources: ${TEST_SRCS}")
else()
  message(WARNING "No test sources found in ${CMAKE_CURRENT_SOURCE_DIR}")
endif()

# Common project source files needed by the tests (adjust as needed)
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

set(TEST_TARGETS "")
foreach(src ${TEST_SRCS})
  get_filename_component(name ${src} NAME_WE)
  set(target test_${name})
  add_executable(${target} ${src} ${COMMON_SRCS})
  target_include_directories(${target} PRIVATE
    ${PROJECT_ROOT}/src
    ${PROJECT_ROOT}/src/storagenode
    ${PROJECT_ROOT}/src/storagenode/hard_disc
    ${PROJECT_ROOT}/src/fs
    ${PROJECT_ROOT}/src/mds
    ${PROJECT_ROOT}/src/debug
    ${PROJECT_ROOT}/src/srm
    ${PROJECT_ROOT}/third_party
  )
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wno-unused-parameter)
  target_link_libraries(${target} PRIVATE Threads::Threads)
  if(ENABLE_ZBSS_LOG)
    target_compile_definitions(${target} PRIVATE ZBSS_ENABLE_LOG)
    # Allow overriding default log level at compile time
    target_compile_definitions(${target} PRIVATE ZBSS_DEFAULT_LOG_LEVEL=${DEFAULT_ZBSS_LOG_LEVEL})
  endif()
  list(APPEND TEST_TARGETS ${target})
endforeach()

# Optional aggregate target to build all tests and make it easy to run
if(TEST_TARGETS)
  add_custom_target(build_tests DEPENDS ${TEST_TARGETS})
endif()
