#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "../proto/common.pb.h"
#include "../../../src/fs/volume/VolumeManager.h"
#include "../../../src/srm/storage_manager/StorageResource.h"

namespace rpc {

struct StorageWriteReply {
    Status status;
    ssize_t bytes{0};
    std::shared_ptr<Inode> inode;
};

struct StorageReadReply {
    Status status;
    ssize_t bytes{0};
    std::string data;
    std::shared_ptr<Inode> inode;
};

class RpcStorageServer {
public:
    RpcStorageServer();

    // Load nodes/libraries from disk, optional reset
    void LoadFromFile(bool initvolumes = false, bool fresh = false);

    // Convert StorageResource nodes to real Volume objects.
    std::vector<VolumeInfo> InitAllNodeVolumes();

    Status RegisterVolume(const std::shared_ptr<Volume>& vol, VolumeType type);

    StorageWriteReply WriteFile(const std::shared_ptr<Inode>& inode,
                                size_t offset,
                                const char* buf,
                                size_t count);

    StorageReadReply ReadFile(const std::shared_ptr<Inode>& inode,
                              size_t offset,
                              size_t count);

    bool ReleaseInodeBlocks(const std::shared_ptr<Inode>& inode);

    std::shared_ptr<VolumeManager> volume_manager() const { return volume_manager_; }
    StorageResource& resource() { return resource_; }

private:
    StorageResource resource_;
    std::shared_ptr<VolumeManager> volume_manager_;
};

class RpcStorageClient {
public:
    explicit RpcStorageClient(const std::shared_ptr<RpcStorageServer>& server)
        : server_(server) {}

    void LoadFromFile(bool initvolumes = false, bool fresh = false) { server_->LoadFromFile(initvolumes, fresh); }
    std::vector<VolumeInfo> InitAllNodeVolumes() { return server_->InitAllNodeVolumes(); }
    Status RegisterVolume(const std::shared_ptr<Volume>& vol, VolumeType type) { return server_->RegisterVolume(vol, type); }
    StorageWriteReply WriteFile(const std::shared_ptr<Inode>& inode,
                                size_t offset,
                                const char* buf,
                                size_t count) { return server_->WriteFile(inode, offset, buf, count); }
    StorageReadReply ReadFile(const std::shared_ptr<Inode>& inode,
                              size_t offset,
                              size_t count) { return server_->ReadFile(inode, offset, count); }
    bool ReleaseInodeBlocks(const std::shared_ptr<Inode>& inode) { return server_->ReleaseInodeBlocks(inode); }

private:
    std::shared_ptr<RpcStorageServer> server_;
};

} // namespace rpc
