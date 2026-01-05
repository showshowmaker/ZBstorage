#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <vector>
#include <fuse.h>
#include <unordered_map>

#include "RpcClients.h"
#include "common/StatusUtils.h"

struct InodeInfo {
    uint64_t inode{0};
    std::string volume_id;
};

class DfsClient {
public:
    explicit DfsClient(MountConfig cfg);
    bool Init();

    int GetAttr(const std::string& path, struct stat* st);
    int ReadDir(const std::string& path, void* buf, fuse_fill_dir_t filler);
    int Open(const std::string& path, int flags, int& out_fd);
    int Create(const std::string& path, int flags, mode_t mode, int& out_fd);
    int Read(int fd, char* buf, size_t size, off_t offset, ssize_t& out_bytes);
    int Write(int fd, const char* buf, size_t size, off_t offset, ssize_t& out_bytes);
    int Close(int fd);

private:
    int StatusToErrno(rpc::StatusCode code) const;
    bool PopulateStatFromInode(const rpc::FindInodeReply& reply, struct stat* st) const;
    rpc::StatusCode LookupInode(const std::string& path, InodeInfo& out_info);

    MountConfig cfg_;
    std::unique_ptr<RpcClients> rpc_;
    int next_fd_{3};
    std::unordered_map<int, InodeInfo> fd_info_;
};
