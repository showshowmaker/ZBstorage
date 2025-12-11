#include <brpc/server.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <sstream>
#include "mds.pb.h"
#include "../../../src/mds/server/Server.h"
#include "../../../src/fs/volume/VolumeRegistry.h"

DEFINE_int32(mds_port, 8010, "Port of MDS server");
DEFINE_int32(mds_idle_timeout, -1, "Idle timeout of mds server (default: infinite)");
DEFINE_int32(mds_thread_num, 4, "Worker threads");
DEFINE_string(mds_data_dir, "/tmp/mds_rpc", "Base dir for inode/bitmap/dir_store");
DEFINE_bool(mds_create_new, true, "Create new metadata store");

namespace {

void LogRequest(const std::string& api, const std::string& detail, const rpc::Status* st = nullptr) {
    std::cout << "[MDS RPC] " << api;
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

class MdsServiceImpl : public rpc::MdsService {
public:
    MdsServiceImpl(const std::string& base_dir, bool create_new)
        : base_dir_(base_dir) {
        std::error_code ec;
        std::filesystem::create_directories(base_dir_, ec);
        const std::string inode_path = base_dir_ + "/inode.dat";
        const std::string bitmap_path = base_dir_ + "/bitmap.dat";
        const std::string dir_store = base_dir_ + "/dir_store";
        std::filesystem::create_directories(dir_store, ec);
        if (create_new) {
            std::filesystem::remove(inode_path, ec);
            std::filesystem::remove(bitmap_path, ec);
            std::filesystem::remove_all(dir_store, ec);
            std::filesystem::create_directories(dir_store, ec);
            // 清理卷注册与 KV 存储，确保无旧数据污染
            std::filesystem::remove(base_dir_ + "/ssd.meta", ec);
            std::filesystem::remove(base_dir_ + "/ssd.data", ec);
            std::filesystem::remove(base_dir_ + "/hdd.meta", ec);
            std::filesystem::remove(base_dir_ + "/hdd.data", ec);
            std::filesystem::remove_all("/tmp/zbstorage_kv", ec);
            std::filesystem::create_directories("/tmp/zbstorage_kv", ec);
        }
        mds_ = std::make_shared<MdsServer>(inode_path, bitmap_path, dir_store, create_new);
        mds_->set_volume_registry(make_file_volume_registry(base_dir_));
    }

    void CreateRoot(::google::protobuf::RpcController*,
                    const rpc::Empty*,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->CreateRoot()));
        LogRequest("CreateRoot", "", response);
    }

    void Mkdir(::google::protobuf::RpcController*,
               const rpc::PathModeRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->Mkdir(request->path(), static_cast<mode_t>(request->mode()))));
        LogRequest("Mkdir", request->path(), response);
    }

    void Rmdir(::google::protobuf::RpcController*,
               const rpc::PathRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->Rmdir(request->path())));
        LogRequest("Rmdir", request->path(), response);
    }

    void CreateFile(::google::protobuf::RpcController*,
                    const rpc::PathModeRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->CreateFile(request->path(), static_cast<mode_t>(request->mode()))));
        LogRequest("CreateFile", request->path(), response);
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
        LogRequest("RemoveFile", request->path(), response->mutable_status());
    }

    void TruncateFile(::google::protobuf::RpcController*,
                      const rpc::PathRequest* request,
                      rpc::Status* response,
                      ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(mds_->TruncateFile(request->path())));
        LogRequest("TruncateFile", request->path(), response);
    }

    void Ls(::google::protobuf::RpcController*,
            const rpc::PathRequest* request,
            rpc::DirectoryListReply* response,
            ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = mds_->FindInodeByPath(request->path());
        if (!inode || inode->file_mode.fields.file_type != static_cast<uint16_t>(FileType::Directory)) {
            response->mutable_status()->CopyFrom(ToStatus(false, "not directory"));
            LogRequest("Ls", request->path(), response->mutable_status());
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
        LogRequest("Ls", request->path(), response->mutable_status());
    }

    void LookupIno(::google::protobuf::RpcController*,
                   const rpc::PathRequest* request,
                   rpc::LookupReply* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        uint64_t ino = mds_->LookupIno(request->path());
        response->set_inode(ino);
        response->mutable_status()->CopyFrom(ToStatus(ino != static_cast<uint64_t>(-1)));
        LogRequest("LookupIno", request->path(), response->mutable_status());
    }

    void FindInode(::google::protobuf::RpcController*,
                   const rpc::PathRequest* request,
                   rpc::FindInodeReply* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = mds_->FindInodeByPath(request->path());
        if (!inode) {
            response->mutable_status()->CopyFrom(ToStatus(false, "inode not found"));
            LogRequest("FindInode", request->path(), response->mutable_status());
            return;
        }
        SerializeInode(*inode, response->mutable_inode());
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("FindInode", request->path(), response->mutable_status());
    }

    void WriteInode(::google::protobuf::RpcController*,
                    const rpc::WriteInodeRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = DeserializeInode(request->inode());
        bool ok = inode && mds_->WriteInode(request->ino(), *inode);
        response->CopyFrom(ToStatus(ok));
        LogRequest("WriteInode", std::to_string(request->ino()), response);
    }

    void CollectColdInodes(::google::protobuf::RpcController*,
                           const rpc::ColdInodeRequest* request,
                           rpc::ColdInodeListReply* response,
                           ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto list = mds_->CollectColdInodes(request->max_candidates(), request->min_age_windows());
        for (auto ino : list) response->add_inodes(ino);
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("CollectColdInodes", "max=" + std::to_string(request->max_candidates()), response->mutable_status());
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
        std::string bits(bitmap->size(), '0');
        for (size_t i = 0; i < bitmap->size(); ++i) {
            bits[i] = bitmap->test(i) ? '1' : '0';
        }
        response->set_bitmap(bits);
        response->set_bit_count(bitmap->size());
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("CollectColdInodesBitmap", "size=" + std::to_string(bitmap->size()), response->mutable_status());
    }

    void CollectColdInodesByAtimePercent(::google::protobuf::RpcController*,
                                         const rpc::ColdInodePercentRequest* request,
                                         rpc::ColdInodeListReply* response,
                                         ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto list = mds_->CollectColdInodesByAtimePercent(request->percent());
        for (auto ino : list) response->add_inodes(ino);
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("CollectColdInodesByAtimePercent", "percent=" + std::to_string(request->percent()), response->mutable_status());
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
        LogRequest("RegisterVolume", vol ? vol->uuid() : "<null>", response->mutable_status());
    }

    void RebuildInodeTable(::google::protobuf::RpcController*,
                           const rpc::Empty*,
                           rpc::Status* response,
                           ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        mds_->RebuildInodeTable();
        response->CopyFrom(ToStatus(true));
        LogRequest("RebuildInodeTable", "", response);
    }

    void GetMetricsProm(::google::protobuf::RpcController* controller,
                        const rpc::Empty*,
                        rpc::MetricsReply* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::ostringstream os;
        uint64_t total = mds_->GetTotalInodes();
        uint64_t root = mds_->GetRootInode();
        os << "# HELP mds_total_inodes Total inodes in MDS\n";
        os << "# TYPE mds_total_inodes gauge\n";
        os << "mds_total_inodes " << total << "\n";
        os << "# HELP mds_root_inode Root inode id\n";
        os << "# TYPE mds_root_inode gauge\n";
        os << "mds_root_inode " << root << "\n";
        auto list = mds_->CollectColdInodes(2, 0);
        os << "# HELP mds_cold_inode_sample Cold inode sample (value=inode id)\n";
        os << "# TYPE mds_cold_inode_sample gauge\n";
        for (size_t i = 0; i < list.size(); ++i) {
            os << "mds_cold_inode_sample{slot=\"" << i << "\"} " << list[i] << "\n";
        }
        response->mutable_status()->CopyFrom(ToStatus(true));
        bool handled_http = false;
        if (auto* cntl = dynamic_cast<brpc::Controller*>(controller)) {
            if (cntl->has_http_request()) {
                cntl->http_response().set_content_type("text/plain");
                cntl->http_response().set_body(os.str());
                handled_http = true;
            }
        }
        if (!handled_http) {
            response->set_text(os.str());
        }
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
