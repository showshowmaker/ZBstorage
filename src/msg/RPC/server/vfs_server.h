#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/dynamic_bitset.hpp>
#include <sys/types.h>
#include "../proto/common.pb.h"
#include "../server/mds_server.h"
#include "../server/storage_server.h"
#include "../../../src/fs/handle/handle.h"

namespace rpc {

// RPC server that fronts the VFS layer and delegates to MDS/Storage RPC clients.
class RpcVfsServer {
public:
    RpcVfsServer(std::shared_ptr<RpcMdsClient> mds_client,
                 std::shared_ptr<RpcStorageClient> storage_client);

    Status Startup();
    Status Shutdown();

    bool CreateRootDirectory();
    bool Mkdir(const std::string& path, mode_t mode);
    bool Rmdir(const std::string& path);
    bool Ls(const std::string& path);
    uint64_t LookupInode(const std::string& path);
    bool CreateFile(const std::string& path, mode_t mode);
    bool RemoveFile(const std::string& path);
    bool RegisterVolume(const std::shared_ptr<Volume>& vol,
                        VolumeType type,
                        int* out_index = nullptr,
                        bool persist_now = false);

    int Open(const std::string& path, int flags, mode_t mode = 0644);
    int Close(int fd);
    int ShutdownFd(int fd);
    off_t Seek(int fd, off_t offset, int whence);
    ssize_t Write(int fd, const char* buf, size_t count);
    ssize_t Read(int fd, char* buf, size_t count);

    std::vector<uint64_t> CollectColdInodes(size_t max_candidates, size_t min_age_windows);
    std::shared_ptr<boost::dynamic_bitset<>> CollectColdInodesBitmap(size_t min_age_windows);
    std::vector<uint64_t> CollectColdInodesByAtimePercent(double percent);

private:
    int acquire_fd_locked();
    void release_fd_locked(int fd);
    FdTableEntry* find_fd_locked(int fd);
    int allocate_fd_locked(std::shared_ptr<Inode> inode, int flags);
    void force_close_handles(uint64_t inode);

    std::shared_ptr<RpcMdsClient> mds_;
    std::shared_ptr<RpcStorageClient> storage_;
    std::unordered_map<int, FdTableEntry> fd_table_;
    boost::dynamic_bitset<> fd_bitmap_;
    std::mutex fd_mutex_;
};

class RpcVfsClient {
public:
    explicit RpcVfsClient(const std::shared_ptr<RpcVfsServer>& server)
        : server_(server) {}

    Status Startup() { return server_->Startup(); }
    Status Shutdown() { return server_->Shutdown(); }

    bool CreateRootDirectory() { return server_->CreateRootDirectory(); }
    bool Mkdir(const std::string& path, mode_t mode) { return server_->Mkdir(path, mode); }
    bool Rmdir(const std::string& path) { return server_->Rmdir(path); }
    bool Ls(const std::string& path) { return server_->Ls(path); }
    uint64_t LookupInode(const std::string& path) { return server_->LookupInode(path); }
    bool CreateFile(const std::string& path, mode_t mode) { return server_->CreateFile(path, mode); }
    bool RemoveFile(const std::string& path) { return server_->RemoveFile(path); }
    bool RegisterVolume(const std::shared_ptr<Volume>& vol,
                        VolumeType type,
                        int* out_index = nullptr,
                        bool persist_now = false) {
        return server_->RegisterVolume(vol, type, out_index, persist_now);
    }
    int Open(const std::string& path, int flags, mode_t mode = 0644) { return server_->Open(path, flags, mode); }
    int Close(int fd) { return server_->Close(fd); }
    int ShutdownFd(int fd) { return server_->ShutdownFd(fd); }
    off_t Seek(int fd, off_t offset, int whence) { return server_->Seek(fd, offset, whence); }
    ssize_t Write(int fd, const char* buf, size_t count) { return server_->Write(fd, buf, count); }
    ssize_t Read(int fd, char* buf, size_t count) { return server_->Read(fd, buf, count); }
    std::vector<uint64_t> CollectColdInodes(size_t max_candidates, size_t min_age_windows) { return server_->CollectColdInodes(max_candidates, min_age_windows); }
    std::shared_ptr<boost::dynamic_bitset<>> CollectColdInodesBitmap(size_t min_age_windows) { return server_->CollectColdInodesBitmap(min_age_windows); }
    std::vector<uint64_t> CollectColdInodesByAtimePercent(double percent) { return server_->CollectColdInodesByAtimePercent(percent); }

private:
    std::shared_ptr<RpcVfsServer> server_;
};

} // namespace rpc
