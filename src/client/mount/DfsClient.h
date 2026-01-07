#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <vector>
#include <fuse.h>
#include <unordered_map>
#include <mutex>

#include "RpcClients.h"
#include "common/StatusUtils.h"

struct InodeInfo {
    uint64_t inode{0};
    std::string node_id;
};

class DfsClient {
public:
    explicit DfsClient(MountConfig cfg);
    bool Init();

    int GetAttr(const std::string& path, struct stat* st);
    int ReadDir(const std::string& path, void* buf, fuse_fill_dir_t filler);
    int Open(const std::string& path, int flags, int& out_fd);
    int Create(const std::string& path, int flags, mode_t mode, int& out_fd);
    int Mkdir(const std::string& path, mode_t mode);
    int Rmdir(const std::string& path);
    int Unlink(const std::string& path);
    int Truncate(const std::string& path, off_t size);
    int Read(int fd, char* buf, size_t size, off_t offset, ssize_t& out_bytes);
    int Write(int fd, const char* buf, size_t size, off_t offset, ssize_t& out_bytes);
    int Close(int fd);

private:
    int StatusToErrno(rpc::StatusCode code) const;
    bool PopulateStat(struct stat* st, bool is_dir) const;
    rpc::StatusCode LookupInode(const std::string& path, InodeInfo& out_info);
    rpc::StatusCode UpdateRemoteSize(uint64_t inode, uint64_t size_bytes);

    MountConfig cfg_;
    std::unique_ptr<RpcClients> rpc_;
    int next_fd_{3};
    std::unordered_map<int, InodeInfo> fd_info_;
    std::unordered_map<uint64_t, uint64_t> inode_size_;
    mutable std::mutex mu_;
};
