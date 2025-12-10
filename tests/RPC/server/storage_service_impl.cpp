#include <brpc/server.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include "storage.pb.h"
#include "../../../src/fs/volume/VolumeManager.h"
#include "../../../src/fs/volume/VolumeRegistry.h"
#include "../../../src/fs/io/LocalStorageGateway.h"
#include "../../../src/srm/storage_manager/StorageResource.h"

DEFINE_int32(storage_port, 8011, "Port of storage server");
DEFINE_int32(storage_idle_timeout, -1, "Idle timeout of storage server");
DEFINE_int32(storage_thread_num, 4, "Worker threads");

namespace {

void LogRequest(const std::string& api, const std::string& detail, const rpc::Status* st = nullptr) {
    std::cout << "[Storage RPC] " << api;
    if (!detail.empty()) std::cout << " " << detail;
    if (st) {
        std::string msg = st->message();
        if (msg.empty()) {
            msg = (st->code() == 0) ? "OK" : "<no-msg>";
        }
        std::cout << " -> code=" << st->code() << " msg=" << msg;
    }
    std::cout << std::endl;
}

rpc::Status ToStatus(bool ok, const std::string& msg = {}) {
    rpc::Status st;
    st.set_code(ok ? 0 : 1);
    st.set_message(ok ? "" : msg);
    return st;
}

std::shared_ptr<Inode> DeserializeInode(const rpc::InodeBlob& blob) {
    size_t off = 0;
    Inode inode;
    if (!Inode::deserialize(reinterpret_cast<const uint8_t*>(blob.data().data()),
                            off, inode, blob.data().size())) {
        return nullptr;
    }
    return std::make_shared<Inode>(inode);
}

void SerializeInode(const Inode& inode, rpc::InodeBlob* out) {
    auto data = inode.serialize();
    out->set_data(data.data(), data.size());
}

std::shared_ptr<Volume> DeserializeVolume(const rpc::VolumeBlob& blob) {
    auto vol = Volume::deserialize(reinterpret_cast<const uint8_t*>(blob.data().data()),
                                   blob.data().size());
    if (!vol) return nullptr;
    return std::shared_ptr<Volume>(std::move(vol));
}

void SerializeVolume(const Volume& vol, rpc::VolumeBlob* out) {
    auto data = vol.serialize();
    out->set_data(data.data(), data.size());
}

} // namespace

class StorageServiceImpl : public rpc::StorageService {
public:
    StorageServiceImpl() {
        // Ensure global storage resource is visible to LocalStorageGateway before any IO.
        g_storage_resource = &resource_;
        // Load storage nodes and pending volumes from file (same as test_vfs_new).
        resource_.loadFromFile(false, false);
        volume_manager_ = std::make_shared<VolumeManager>();
        volume_manager_->set_default_gateway(std::make_shared<LocalStorageGateway>());
        // Initialize all node volumes once and register into VolumeManager, also cache for ListAllVolumes.
        const size_t kMaxCachedVolumes = 16; // avoid oversized RPC payloads
        while (true) {
            auto pair = resource_.initOneNodeVolume();
            if (!pair.first && !pair.second) break;
            if (pair.first) {
                volume_manager_->register_volume(pair.first);
                if (volume_cache_.size() < kMaxCachedVolumes) {
                    volume_cache_.push_back({pair.first, VolumeType::SSD});
                }
            }
            if (pair.second) {
                volume_manager_->register_volume(pair.second);
                if (volume_cache_.size() < kMaxCachedVolumes) {
                    volume_cache_.push_back({pair.second, VolumeType::HDD});
                }
            }
            if (volume_cache_.size() >= kMaxCachedVolumes) {
                break;
            }
        }
    }

    void RegisterVolume(::google::protobuf::RpcController*,
                        const rpc::StorageRegisterVolumeRequest* request,
                        rpc::Status* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto vol = DeserializeVolume(request->volume());
        if (!vol) {
            response->CopyFrom(ToStatus(false, "volume deserialize failed"));
            LogRequest("RegisterVolume", "<null>", response);
            return;
        }
        volume_manager_->register_volume(vol);
        response->CopyFrom(ToStatus(true));
        LogRequest("RegisterVolume", vol->uuid(), response);
    }

    void ListAllVolumes(::google::protobuf::RpcController*,
                        const rpc::Empty*,
                        rpc::VolumeListReply* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        for (const auto& v : volume_cache_) {
            rpc::VolumeInfo info;
            SerializeVolume(*v.volume, info.mutable_volume());
            info.set_type(static_cast<uint32_t>(v.type));
            auto* out = response->add_volumes();
            out->Swap(&info);
        }
        auto st = ToStatus(true);
        response->mutable_status()->CopyFrom(st);
        LogRequest("ListAllVolumes", "count=" + std::to_string(response->volumes_size()), &st);
    }

    void WriteFile(::google::protobuf::RpcController*,
                   const rpc::StorageIORequest* request,
                   rpc::StorageWriteReply* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = DeserializeInode(request->inode());
        if (!inode) {
            response->mutable_status()->CopyFrom(ToStatus(false, "inode deserialize failed"));
            LogRequest("WriteFile", "inode deserialize failed", response->mutable_status());
            return;
        }
        ssize_t bytes = volume_manager_->write_file(inode,
                                                    request->offset(),
                                                    request->data().data(),
                                                    request->data().size());
        if (bytes < 0) {
            response->mutable_status()->CopyFrom(ToStatus(false, "write failed"));
            response->set_bytes(bytes);
            LogRequest("WriteFile", "offset=" + std::to_string(request->offset()), response->mutable_status());
            return;
        }
        response->set_bytes(bytes);
        SerializeInode(*inode, response->mutable_inode());
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("WriteFile", "offset=" + std::to_string(request->offset()) + " bytes=" + std::to_string(bytes), response->mutable_status());
    }

    void ReadFile(::google::protobuf::RpcController*,
                  const rpc::StorageIORequest* request,
                  rpc::StorageReadReply* response,
                  ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = DeserializeInode(request->inode());
        if (!inode) {
            response->mutable_status()->CopyFrom(ToStatus(false, "inode deserialize failed"));
            LogRequest("ReadFile", "inode deserialize failed", response->mutable_status());
            return;
        }
        std::string buf(request->data().size(), '\0');
        ssize_t bytes = volume_manager_->read_file(inode,
                                                   request->offset(),
                                                   buf.data(),
                                                   buf.size());
        if (bytes < 0) {
            response->mutable_status()->CopyFrom(ToStatus(false, "read failed"));
            response->set_bytes(bytes);
            LogRequest("ReadFile", "offset=" + std::to_string(request->offset()), response->mutable_status());
            return;
        }
        response->set_bytes(bytes);
        response->set_data(buf.data(), static_cast<size_t>(bytes));
        SerializeInode(*inode, response->mutable_inode());
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("ReadFile", "offset=" + std::to_string(request->offset()) + " bytes=" + std::to_string(bytes), response->mutable_status());
    }

    void ReleaseInodeBlocks(::google::protobuf::RpcController*,
                            const rpc::InodeBlob* request,
                            rpc::Status* response,
                            ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = DeserializeInode(*request);
        bool ok = inode && volume_manager_->release_inode_blocks(inode);
        response->CopyFrom(ToStatus(ok));
        LogRequest("ReleaseInodeBlocks", inode ? std::to_string(inode->inode) : "<null>", response);
    }

private:
    std::shared_ptr<VolumeManager> volume_manager_;
    StorageResource resource_;
    struct VolumeCacheEntry {
        std::shared_ptr<Volume> volume;
        VolumeType type;
    };
    std::vector<VolumeCacheEntry> volume_cache_;
};

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server server;
    StorageServiceImpl svc;
    if (server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add storage service" << std::endl;
        return -1;
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_storage_idle_timeout;
    options.num_threads = FLAGS_storage_thread_num;
    if (server.Start(FLAGS_storage_port, &options) != 0) {
        std::cerr << "Failed to start storage server" << std::endl;
        return -1;
    }
    std::cout << "Storage server listening on " << FLAGS_storage_port << std::endl;
    server.RunUntilAskedToQuit();
    return 0;
}
