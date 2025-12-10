#include <brpc/server.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <vector>
#include "mds.pb.h"
#include "../../../src/mds/server/Server.h"
#include "../../../src/fs/volume/VolumeRegistry.h"

DEFINE_int32(mds_port, 8010, "Port of MDS server");
DEFINE_int32(mds_idle_timeout, -1, "Idle timeout of mds server (default: infinite)");
DEFINE_int32(mds_thread_num, 4, "Worker threads");
DEFINE_string(mds_data_dir, "/tmp/mds_rpc", "Base dir for inode/bitmap/dir_store");
DEFINE_bool(mds_create_new, true, "Create new metadata store");

namespace {

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

class MdsServiceImpl : public rpc::MdsService {
public:
    MdsServiceImpl(const std::string& base_dir, bool create_new)
        : base_dir_(base_dir) {
        const std::string inode_path = base_dir_ + "/inode.dat";
        const std::string bitmap_path = base_dir_ + "/bitmap.dat";
        const std::string dir_store = base_dir_ + "/dir_store";
        mds_ = std::make_shared<MdsServer>(inode_path, bitmap_path, dir_store, create_new);
        mds_->set_volume_registry(make_file_volume_registry(base_dir_));
    }

    void CreateRoot(::google::protobuf::RpcController*,
                    const rpc::Empty*,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->CreateRoot()));
    }

    void Mkdir(::google::protobuf::RpcController*,
               const rpc::PathModeRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->Mkdir(request->path(), static_cast<mode_t>(request->mode()))));
    }

    void Rmdir(::google::protobuf::RpcController*,
               const rpc::PathRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->Rmdir(request->path())));
    }

    void CreateFile(::google::protobuf::RpcController*,
                    const rpc::PathModeRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->CreateFile(request->path(), static_cast<mode_t>(request->mode()))));
    }

    void RemoveFile(::google::protobuf::RpcController*,
                    const rpc::PathRequest* request,
                    rpc::RemoveFileReply* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        uint64_t ino = mds_->LookupIno(request->path());
        bool ok = mds_->RemoveFile(request->path());
        response->mutable_status()->CopyFrom(ToStatus(ok));
        if (ok && ino != static_cast<uint64_t>(-1)) {
            response->add_detached_inodes(ino);
        }
    }

    void TruncateFile(::google::protobuf::RpcController*,
                      const rpc::PathRequest* request,
                      rpc::Status* response,
                      ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->TruncateFile(request->path())));
    }

    void Ls(::google::protobuf::RpcController*,
            const rpc::PathRequest* request,
            rpc::DirectoryListReply* response,
            ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = mds_->FindInodeByPath(request->path());
        if (!inode || inode->getFileType() != FileType::Directory) {
            response->mutable_status()->CopyFrom(ToStatus(false, "not directory"));
            return;
        }
        auto entries = mds_->ReadDirectoryEntries(inode);
        for (const auto& e : entries) {
            auto* d = response->add_entries();
            d->set_inode(e.inode);
            d->set_type(static_cast<uint32_t>(e.file_type));
            d->set_name(std::string(e.name, e.name + e.name_len));
        }
        response->mutable_status()->CopyFrom(ToStatus(true));
    }

    void LookupIno(::google::protobuf::RpcController*,
                   const rpc::PathRequest* request,
                   rpc::LookupReply* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        uint64_t ino = mds_->LookupIno(request->path());
        response->set_inode(ino);
        response->mutable_status()->CopyFrom(ToStatus(ino != static_cast<uint64_t>(-1)));
    }

    void FindInode(::google::protobuf::RpcController*,
                   const rpc::PathRequest* request,
                   rpc::FindInodeReply* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = mds_->FindInodeByPath(request->path());
        if (!inode) {
            response->mutable_status()->CopyFrom(ToStatus(false, "inode not found"));
            return;
        }
        SerializeInode(*inode, response->mutable_inode());
        response->mutable_status()->CopyFrom(ToStatus(true));
    }

    void WriteInode(::google::protobuf::RpcController*,
                    const rpc::WriteInodeRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = DeserializeInode(request->inode());
        bool ok = inode && mds_->WriteInode(request->ino(), *inode);
        response->CopyFrom(ToStatus(ok));
    }

    void CollectColdInodes(::google::protobuf::RpcController*,
                           const rpc::ColdInodeRequest* request,
                           rpc::ColdInodeListReply* response,
                           ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto list = mds_->CollectColdInodes(request->max_candidates(), request->min_age_windows());
        for (auto ino : list) response->add_inodes(ino);
        response->mutable_status()->CopyFrom(ToStatus(true));
    }

    void CollectColdInodesBitmap(::google::protobuf::RpcController*,
                                 const rpc::ColdInodeBitmapRequest* request,
                                 rpc::ColdInodeBitmapReply* response,
                                 ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto bitmap = mds_->CollectColdInodesBitmap(request->min_age_windows());
        if (!bitmap) {
            response->mutable_status()->CopyFrom(ToStatus(false, "bitmap null"));
            return;
        }
        std::string bits = bitmap->to_string();
        response->set_bitmap(bits);
        response->set_bit_count(bitmap->size());
        response->mutable_status()->CopyFrom(ToStatus(true));
    }

    void CollectColdInodesByAtimePercent(::google::protobuf::RpcController*,
                                         const rpc::ColdInodePercentRequest* request,
                                         rpc::ColdInodeListReply* response,
                                         ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto list = mds_->CollectColdInodesByAtimePercent(request->percent());
        for (auto ino : list) response->add_inodes(ino);
        response->mutable_status()->CopyFrom(ToStatus(true));
    }

    void RegisterVolume(::google::protobuf::RpcController*,
                        const rpc::RegisterVolumeRequest* request,
                        rpc::RegisterVolumeReply* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto vol = DeserializeVolume(request->volume());
        int index = -1;
        bool ok = vol && mds_->RegisterVolume(vol,
                                              static_cast<VolumeType>(request->type()),
                                              &index,
                                              request->persist_now());
        response->mutable_status()->CopyFrom(ToStatus(ok));
        response->set_index(ok ? index : -1);
    }

    void RebuildInodeTable(::google::protobuf::RpcController*,
                           const rpc::Empty*,
                           rpc::Status* response,
                           ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        mds_->RebuildInodeTable();
        response->CopyFrom(ToStatus(true));
    }

private:
    std::string base_dir_;
    std::shared_ptr<MdsServer> mds_;
};

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server server;
    MdsServiceImpl svc(FLAGS_mds_data_dir, FLAGS_mds_create_new);
    if (server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add mds service" << std::endl;
        return -1;
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_mds_idle_timeout;
    options.num_threads = FLAGS_mds_thread_num;
    if (server.Start(FLAGS_mds_port, &options) != 0) {
        std::cerr << "Failed to start mds server" << std::endl;
        return -1;
    }
    std::cout << "MDS server listening on " << FLAGS_mds_port << std::endl;
    server.RunUntilAskedToQuit();
    return 0;
}
