// src/client/VFS_dist.h
#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "proto/mds.pb.h"
#include "proto/storage.pb.h"

// 如果你现在的代码里已经有 MO_* 宏，这段保护逻辑不会覆盖
#include <fcntl.h>
#ifndef MO_RDONLY
#define MO_RDONLY O_RDONLY
#define MO_WRONLY O_WRONLY
#define MO_RDWR   O_RDWR
#define MO_CREAT  O_CREAT
#endif

struct FileHandle {
    uint64_t inode_id = 0;
    uint64_t offset   = 0;
};

class DistFileSystem {
public:
    explicit DistFileSystem(const std::string& mds_addr);

    // 禁用拷贝，只允许一个实例管理 RPC 连接
    DistFileSystem(const DistFileSystem&) = delete;
    DistFileSystem& operator=(const DistFileSystem&) = delete;

    // POSIX 风格接口
    int open(const std::string& path, int flags, int mode);
    ssize_t write(int fd, const void* buf, size_t count);
    ssize_t read(int fd, void* buf, size_t count);
    off_t seek(int fd, off_t offset, int whence);
    int close(int fd);

private:
    brpc::Channel mds_channel_;
    std::unique_ptr<fs::mds::MdsService_Stub> mds_stub_;

    std::unordered_map<int, FileHandle> fd_table_;
    int next_fd_ = 3;  // 0/1/2 预留给 stdin/out/err
    std::mutex mu_;

    // 缓存每个 Storage 节点的 channel，避免每次重建
    brpc::Channel* GetOrCreateStorageChannel(const std::string& addr);

    std::map<std::string, std::unique_ptr<brpc::Channel>> storage_channels_;
};
