#include "storage_server.h"
#include "../../../src/fs/io/LocalStorageGateway.h"
#include "../../../src/debug/ZBLog.h"

namespace rpc {

RpcStorageServer::RpcStorageServer()
    : volume_manager_(std::make_shared<VolumeManager>()) {
    // Use local gateway to mimic real I/O path.
    volume_manager_->set_default_gateway(std::make_shared<LocalStorageGateway>());
}

void RpcStorageServer::LoadFromFile(bool initvolumes, bool fresh) {
    resource_.loadFromFile(initvolumes, fresh);
}

std::vector<VolumeInfo> RpcStorageServer::InitAllNodeVolumes() {
    std::vector<VolumeInfo> vols;
    while (true) {
        auto node_volumes = resource_.initOneNodeVolume();
        if (!node_volumes.first && !node_volumes.second) {
            break;
        }
        if (node_volumes.first) {
            vols.push_back({node_volumes.first, VolumeType::SSD});
        }
        if (node_volumes.second) {
            vols.push_back({node_volumes.second, VolumeType::HDD});
        }
    }
    return vols;
}

Status RpcStorageServer::RegisterVolume(const std::shared_ptr<Volume>& vol, VolumeType) {
    if (!vol) return Status::Error(1, "Volume is null");
    volume_manager_->register_volume(vol);
    return Status::Ok();
}

StorageWriteReply RpcStorageServer::WriteFile(const std::shared_ptr<Inode>& inode,
                                              size_t offset,
                                              const char* buf,
                                              size_t count) {
    StorageWriteReply reply;
    if (!inode || !buf) {
        reply.status = Status::Error(1, "Invalid input");
        return reply;
    }
    // Work on a copy so RPC boundary is explicit.
    auto working = std::make_shared<Inode>(*inode);
    ssize_t written = volume_manager_->write_file(working, offset, buf, count);
    if (written < 0) {
        reply.status = Status::Error(2, "VolumeManager::write_file failed");
        reply.bytes = written;
        return reply;
    }
    reply.bytes = written;
    reply.inode = working;
    reply.status = Status::Ok();
    return reply;
}

StorageReadReply RpcStorageServer::ReadFile(const std::shared_ptr<Inode>& inode,
                                            size_t offset,
                                            size_t count) {
    StorageReadReply reply;
    if (!inode) {
        reply.status = Status::Error(1, "Invalid inode");
        return reply;
    }
    auto working = std::make_shared<Inode>(*inode);
    std::string buf(count, '\0');
    ssize_t bytes = volume_manager_->read_file(working, offset, buf.data(), count);
    if (bytes < 0) {
        reply.status = Status::Error(2, "VolumeManager::read_file failed");
        reply.bytes = bytes;
        return reply;
    }
    buf.resize(static_cast<size_t>(bytes));
    reply.status = Status::Ok();
    reply.bytes = bytes;
    reply.data = std::move(buf);
    reply.inode = working;
    return reply;
}

bool RpcStorageServer::ReleaseInodeBlocks(const std::shared_ptr<Inode>& inode) {
    if (!inode) return false;
    auto working = std::make_shared<Inode>(*inode);
    return volume_manager_->release_inode_blocks(working);
}

} // namespace rpc

#ifndef RPC_BUILD_LIBRARY_ONLY
int main() {
    // Standalone build target placeholder to satisfy tests/CMakeLists globbed executables.
    return 0;
}
#endif
