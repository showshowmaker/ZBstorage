#pragma once
#include <memory>
#include <string>
#include <vector>
#include <sys/types.h>
#include "../proto/common.pb.h"
#include "../../../src/mds/server/Server.h"

namespace rpc {

struct RemoveFileReply {
    Status status;
    // inode ids that should be force-closed on the client side.
    std::vector<uint64_t> detached_inodes;
};

class RpcMdsServer {
public:
    RpcMdsServer(const std::string& inode_path,
                 const std::string& bitmap_path,
                 const std::string& dir_store_base,
                 bool create_new,
                 const std::string& registry_base = ".");

    Status CreateRoot();
    Status Mkdir(const std::string& path, mode_t mode);
    Status Rmdir(const std::string& path);
    Status CreateFile(const std::string& path, mode_t mode);
    RemoveFileReply RemoveFile(const std::string& path);
    Status TruncateFile(const std::string& path);
    Status Ls(const std::string& path, DirectoryList& out);
    uint64_t LookupIno(const std::string& path);
    std::shared_ptr<Inode> FindInode(const std::string& path);
    Status WriteInode(uint64_t ino, const Inode& inode);
    uint64_t GetTotalInodes() const;
    uint64_t GetRootInode() const;
    ColdInodeList CollectColdInodes(size_t max_candidates, size_t min_age_windows);
    ColdInodeBitmap CollectColdInodesBitmap(size_t min_age_windows);
    ColdInodeList CollectColdInodesByPercent(double percent);
    Status RegisterVolume(const std::shared_ptr<Volume>& vol,
                          VolumeType type,
                          int* out_index = nullptr,
                          bool persist_now = false);
    Status RebuildInodeTable();

    std::shared_ptr<MdsServer> raw() const { return mds_; }

private:
    std::shared_ptr<MdsServer> mds_;
};

class RpcMdsClient {
public:
    explicit RpcMdsClient(const std::shared_ptr<RpcMdsServer>& server)
        : server_(server) {}

    Status CreateRoot();
    Status Mkdir(const std::string& path, mode_t mode);
    Status Rmdir(const std::string& path);
    Status CreateFile(const std::string& path, mode_t mode);
    RemoveFileReply RemoveFile(const std::string& path);
    Status TruncateFile(const std::string& path);
    Status Ls(const std::string& path, DirectoryList& out);
    uint64_t LookupIno(const std::string& path);
    std::shared_ptr<Inode> FindInode(const std::string& path);
    Status WriteInode(uint64_t ino, const Inode& inode);
    uint64_t GetTotalInodes() const;
    uint64_t GetRootInode() const;
    ColdInodeList CollectColdInodes(size_t max_candidates, size_t min_age_windows);
    ColdInodeBitmap CollectColdInodesBitmap(size_t min_age_windows);
    ColdInodeList CollectColdInodesByPercent(double percent);
    Status RegisterVolume(const std::shared_ptr<Volume>& vol,
                          VolumeType type,
                          int* out_index = nullptr,
                          bool persist_now = false);
    Status RebuildInodeTable();

private:
    std::shared_ptr<RpcMdsServer> server_;
};

} // namespace rpc
