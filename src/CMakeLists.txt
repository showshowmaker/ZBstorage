cmake_minimum_required(VERSION 3.10)
project(DFS_Project CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- 1. 查找 PkgConfig 工具本身 ---
find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)
# --- 2. 使用 pkg-config 查找所有外部依赖 ---
# 查找 Protobuf (proto 模块需要)
# pkg_check_modules(PROTOBUF REQUIRED protobuf)


# 查找 bRPC (lib_dfs_client 和 server 模块需要)
# 
# 关键：一个写得好的 brpc.pc 文件会将其所有依赖
# (gflags, leveldb, openssl, zlib, dl, pthread) 
# 都包含在 BRPC_LIBRARIES 变量中。
# 这让我们不再需要单独查找它们。
pkg_check_modules(BRPC REQUIRED brpc)
pkg_check_modules(FUSE REQUIRED fuse)

# --- 3. 设置全局头文件路径 ---
# 将找到的头文件路径添加到全局，以便所有子模块都能访问
include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${BRPC_INCLUDE_DIRS})
include_directories(${FUSE_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR}/../include)

# --- 4. 包含所有子模块 ---
# 依赖顺序：proto -> rpc -> fuse
add_subdirectory(msg/proto)
add_subdirectory(msg/RPC)
add_subdirectory(client/fuse)
add_subdirectory(srm)

add_library(StorageResourceAPI STATIC StorageResourceAPI.cpp)

# Add MDS subdirectory so mds server and demo target are built when configuring from src/
add_subdirectory(mds)
