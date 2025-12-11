#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <boost/dynamic_bitset.hpp>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <sstream>
#include "vfs.pb.h"
#include "mds.pb.h"
#include "storage.pb.h"
#include "../../../src/mds/inode/InodeTimestamp.h"
#include "../../../src/fs/handle/handle.h"
#include "../../../src/fs/volume/Volume.h"
#include "../../../src/fs/volume/VolumeRegistry.h"

DEFINE_int32(vfs_port, 8012, "Port of vfs server");
DEFINE_int32(vfs_idle_timeout, -1, "Idle timeout of vfs server");
DEFINE_int32(vfs_thread_num, 4, "Worker threads");
DEFINE_string(mds_addr, "127.0.0.1:8010", "mds server address");
DEFINE_string(storage_addr, "127.0.0.1:8011", "storage server address");

namespace {

void LogRequest(const std::string& api, const std::string& detail, const rpc::Status* st = nullptr) {
    std::cout << "[VFS RPC] " << api;
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

struct FdEntry {
    std::shared_ptr<Inode> inode;
    size_t offset{0};
    int flags{0};
    uint32_t ref_count{1};
};

class VfsServiceImpl : public rpc::VfsService {
public:
    VfsServiceImpl(const std::string& mds_addr, const std::string& storage_addr) {
        brpc::ChannelOptions opt;
        opt.protocol = "baidu_std";
        if (mds_channel_.Init(mds_addr.c_str(), &opt) != 0) {
            throw std::runtime_error("Init mds channel failed");
        }
        if (storage_channel_.Init(storage_addr.c_str(), &opt) != 0) {
            throw std::runtime_error("Init storage channel failed");
        }
        mds_addr_ = mds_addr;
        storage_addr_ = storage_addr;
        if (fd_bitmap_.empty()) {
            fd_bitmap_.resize(4096, true);
            for (int fd : {0, 1, 2}) {
                fd_bitmap_.reset(fd);
            }
        }
    }

    void Startup(::google::protobuf::RpcController* controller,
                 const rpc::Empty*,
                 rpc::Status* response,
                 ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        // 1) 获取存储侧的卷列表并注册
        rpc::StorageService_Stub storage_stub(&storage_channel_);
        rpc::VolumeListReply vol_reply;
        brpc::Controller cvol;
        rpc::Empty empty;
        storage_stub.ListAllVolumes(&cvol, &empty, &vol_reply, nullptr);
        if (vol_reply.status().code() == 0) {
            for (const auto& v : vol_reply.volumes()) {
                rpc::RegisterVolumeRequest req;
                req.mutable_volume()->CopyFrom(v.volume());
                req.set_type(v.type());
                req.set_persist_now(false);
                rpc::RegisterVolumeReply rep;
                brpc::Controller creg;
                // 注册到 MDS（存储侧已在启动时注册到 VolumeManager）
                rpc::MdsService_Stub mds_stub(&mds_channel_);
                mds_stub.RegisterVolume(&creg, &req, &rep, nullptr);
                LogRequest("RegisterVolume@startup", "type=" + std::to_string(v.type()), rep.mutable_status());
            }
        }
        // 如果没有任何卷，则补一个 fallback 卷（行为与 test_vfs_new 相同）
        if (vol_reply.volumes_size() == 0) {
            auto fallback_vol = std::make_shared<Volume>("vol-1", "node-1", 4096);
            rpc::RegisterVolumeRequest req;
            SerializeVolume(*fallback_vol, req.mutable_volume());
            req.set_type(static_cast<uint32_t>(VolumeType::SSD));
            req.set_persist_now(false);
            rpc::RegisterVolumeReply rep;
            brpc::Controller creg;
            rpc::MdsService_Stub mds_stub(&mds_channel_);
            mds_stub.RegisterVolume(&creg, &req, &rep, nullptr);
            LogRequest("RegisterVolume@startup", "fallback uuid=" + fallback_vol->uuid(), rep.mutable_status());
        }

        // 2) 创建根目录（失败则尝试重建 inode 表后重试）
        rpc::MdsService_Stub stub(&mds_channel_);
        rpc::Status st;
        brpc::Controller cntl;
        stub.CreateRoot(&cntl, &empty, &st, nullptr);
        if (st.code() != 0) {
            // 如果 root 创建失败，尝试重建 inode 表后再试一次
            rpc::Status rebuild;
            brpc::Controller c2;
            stub.RebuildInodeTable(&c2, &empty, &rebuild, nullptr);
            if (rebuild.code() == 0) {
                brpc::Controller c3;
                stub.CreateRoot(&c3, &empty, &st, nullptr);
            }
        }
        response->CopyFrom(st);
        LogRequest("Startup", "", response);
    }

    void Shutdown(::google::protobuf::RpcController*,
                  const rpc::Empty*,
                  rpc::Status* response,
                  ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(true));
        LogRequest("Shutdown", "", response);
    }

    void CreateRootDirectory(::google::protobuf::RpcController* controller,
                             const rpc::Empty*,
                             rpc::Status* response,
                             ::google::protobuf::Closure* done) override {
        Startup(controller, nullptr, response, done);
    }

    void Mkdir(::google::protobuf::RpcController*,
               const rpc::PathModeRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.Mkdir(&cntl, request, response, nullptr);
        LogRequest("Mkdir", request->path(), response);
    }

    void Rmdir(::google::protobuf::RpcController*,
               const rpc::PathRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.Rmdir(&cntl, request, response, nullptr);
        LogRequest("Rmdir", request->path(), response);
    }

    void Ls(::google::protobuf::RpcController*,
            const rpc::PathRequest* request,
            rpc::DirectoryListReply* response,
            ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.Ls(&cntl, request, response, nullptr);
        if (response->status().code() == 0) {
            for (const auto& e : response->entries()) {
                std::cout << e.name() << std::endl;
            }
        }
        LogRequest("Ls", request->path(), response->mutable_status());
    }

    void LookupInode(::google::protobuf::RpcController*,
                     const rpc::PathRequest* request,
                     rpc::LookupReply* response,
                     ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.LookupIno(&cntl, request, response, nullptr);
        LogRequest("LookupInode", request->path(), response->mutable_status());
    }

    void CreateFile(::google::protobuf::RpcController*,
                    const rpc::PathModeRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.CreateFile(&cntl, request, response, nullptr);
        LogRequest("CreateFile", request->path(), response);
    }

    void RemoveFile(::google::protobuf::RpcController*,
                    const rpc::PathRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        rpc::RemoveFileReply reply;
        brpc::Controller cntl;
        stub.RemoveFile(&cntl, request, &reply, nullptr);
        for (auto ino : reply.detached_inodes()) {
            force_close_handles(ino);
        }
        response->CopyFrom(reply.status());
        LogRequest("RemoveFile", request->path(), response);
    }

    void RegisterVolume(::google::protobuf::RpcController*,
                        const rpc::RegisterVolumeRequest* request,
                        rpc::RegisterVolumeReply* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::StorageService_Stub storage_stub(&storage_channel_);
        rpc::Status st_storage;
        brpc::Controller c1;
        rpc::StorageRegisterVolumeRequest storage_req;
        storage_req.mutable_volume()->CopyFrom(request->volume());
        storage_req.set_type(request->type());
        storage_stub.RegisterVolume(&c1, &storage_req, &st_storage, nullptr);

        rpc::MdsService_Stub mds_stub(&mds_channel_);
        brpc::Controller c2;
        mds_stub.RegisterVolume(&c2, request, response, nullptr);
        if (st_storage.code() != 0 || response->status().code() != 0) {
            response->mutable_status()->set_code(1);
            response->mutable_status()->set_message("register volume failed");
        }
        LogRequest("RegisterVolume", "type=" + std::to_string(request->type()), response->mutable_status());
    }

    void Open(::google::protobuf::RpcController*,
              const rpc::OpenRequest* request,
              rpc::IOReplyFD* response,
              ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto inode = fetch_inode(request->path(), request->flags(), request->mode());
        if (!inode) {
            response->mutable_status()->CopyFrom(ToStatus(false, "open failed"));
            response->set_bytes(-1);
            return;
        }
        std::lock_guard<std::mutex> lk(fd_mutex_);
        int fd = allocate_fd_locked(inode, request->flags());
        if (fd < 0) {
            response->mutable_status()->CopyFrom(ToStatus(false, "fd allocate failed"));
            response->set_bytes(-1);
            return;
        }
        response->set_bytes(fd);
        response->mutable_status()->CopyFrom(ToStatus(true));
        LogRequest("Open", request->path() + " flags=" + std::to_string(request->flags()), response->mutable_status());
    }

    void Close(::google::protobuf::RpcController*,
               const rpc::FdRequest* request,
               rpc::Status* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(shutdown_fd(request->fd()) == 0));
        LogRequest("Close", "fd=" + std::to_string(request->fd()), response);
    }

    void ShutdownFd(::google::protobuf::RpcController*,
                    const rpc::FdRequest* request,
                    rpc::Status* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->CopyFrom(ToStatus(shutdown_fd(request->fd()) == 0));
        LogRequest("ShutdownFd", "fd=" + std::to_string(request->fd()), response);
    }

    void Seek(::google::protobuf::RpcController*,
              const rpc::SeekRequest* request,
              rpc::SeekReply* response,
              ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        off_t off = seek_fd(request->fd(), request->offset(), request->whence());
        response->set_offset(off);
        response->mutable_status()->CopyFrom(ToStatus(off >= 0));
        LogRequest("Seek", "fd=" + std::to_string(request->fd()) + " off=" + std::to_string(request->offset()), response->mutable_status());
    }

    void Write(::google::protobuf::RpcController*,
               const rpc::IORequestFD* request,
               rpc::IOReplyFD* response,
               ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        ssize_t bytes = write_fd(request->fd(),
                                 request->data().data(),
                                 request->data().size());
        response->set_bytes(bytes);
        response->mutable_status()->CopyFrom(ToStatus(bytes >= 0));
        LogRequest("Write", "fd=" + std::to_string(request->fd()) + " bytes=" + std::to_string(bytes), response->mutable_status());
    }

    void Read(::google::protobuf::RpcController*,
              const rpc::IORequestFD* request,
              rpc::IOReplyFD* response,
              ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        size_t count = request->data().size();
        std::string buf(count, '\0');
        ssize_t bytes = read_fd(request->fd(), buf.data(), buf.size());
        response->set_bytes(bytes);
        if (bytes > 0) {
            response->set_data(buf.data(), static_cast<size_t>(bytes));
        }
        response->mutable_status()->CopyFrom(ToStatus(bytes >= 0));
        LogRequest("Read", "fd=" + std::to_string(request->fd()) + " bytes=" + std::to_string(bytes), response->mutable_status());
    }

    void CollectColdInodes(::google::protobuf::RpcController*,
                           const rpc::ColdInodeRequest* request,
                           rpc::ColdInodeListReply* response,
                           ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.CollectColdInodes(&cntl, request, response, nullptr);
        LogRequest("CollectColdInodes", "max=" + std::to_string(request->max_candidates()), response->mutable_status());
    }

    void CollectColdInodesBitmap(::google::protobuf::RpcController*,
                                 const rpc::ColdInodeBitmapRequest* request,
                                 rpc::ColdInodeBitmapReply* response,
                                 ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.CollectColdInodesBitmap(&cntl, request, response, nullptr);
        LogRequest("CollectColdInodesBitmap", "", response->mutable_status());
    }

    void CollectColdInodesByAtimePercent(::google::protobuf::RpcController*,
                                         const rpc::ColdInodePercentRequest* request,
                                         rpc::ColdInodeListReply* response,
                                         ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rpc::MdsService_Stub stub(&mds_channel_);
        brpc::Controller cntl;
        stub.CollectColdInodesByAtimePercent(&cntl, request, response, nullptr);
        LogRequest("CollectColdInodesByAtimePercent", "percent=" + std::to_string(request->percent()), response->mutable_status());
    }

    void GetMetricsProm(::google::protobuf::RpcController* controller,
                        const rpc::Empty*,
                        rpc::MetricsReply* response,
                        ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::ostringstream os;
        // Volumes via storage RPC
        rpc::StorageService_Stub storage_stub(&storage_channel_);
        rpc::VolumeListReply vol_reply;
        rpc::Empty empty;
        brpc::Controller cvol;
        storage_stub.ListAllVolumes(&cvol, &empty, &vol_reply, nullptr);
        os << "# HELP vfs_volume_count Volume count discovered via storage RPC\n";
        os << "# TYPE vfs_volume_count gauge\n";
        if (!cvol.Failed() && vol_reply.status().code() == 0) {
            os << "vfs_volume_count " << vol_reply.volumes_size() << "\n";
            os << "# HELP vfs_volume_bytes Volume size in bytes (sampled)\n";
            os << "# TYPE vfs_volume_bytes gauge\n";
            os << "# HELP vfs_volume_used_bytes Used size in bytes (sampled)\n";
            os << "# TYPE vfs_volume_used_bytes gauge\n";
            size_t limit = std::min<size_t>(2, vol_reply.volumes_size());
            for (size_t i = 0; i < limit; ++i) {
                const auto& v = vol_reply.volumes(static_cast<int>(i));
                auto vol = DeserializeVolume(v.volume());
                if (!vol) continue;
                const auto& bm = vol->block_manager();
                auto total_bytes = bm.total_blocks() * bm.block_size();
                auto used_bytes = vol->used_blocks() * bm.block_size();
                os << "vfs_volume_bytes{uuid=\"" << vol->uuid()
                   << "\",type=\"" << v.type()
                   << "\",slot=\"" << i << "\"} " << total_bytes << "\n";
                os << "vfs_volume_used_bytes{uuid=\"" << vol->uuid()
                   << "\",type=\"" << v.type()
                   << "\",slot=\"" << i << "\"} " << used_bytes << "\n";
            }
        } else {
            os << "vfs_volume_count 0\n";
        }
        response->mutable_status()->CopyFrom(ToStatus(true));
        response->set_text(os.str());
        if (auto* cntl = dynamic_cast<brpc::Controller*>(controller)) {
            cntl->http_response().set_content_type("text/plain");
        }
    }

private:
    std::shared_ptr<Inode> fetch_inode(const std::string& path, int flags, mode_t mode) {
        rpc::MdsService_Stub stub(&mds_channel_);
        rpc::FindInodeReply find_reply;
        rpc::PathRequest req;
        req.set_path(path);
        brpc::Controller cntl;
        stub.FindInode(&cntl, &req, &find_reply, nullptr);
        if (find_reply.status().code() != 0) {
            if (!(flags & MO_CREAT)) {
                return nullptr;
            }
            rpc::PathModeRequest create_req;
            create_req.set_path(path);
            create_req.set_mode(mode);
            rpc::Status st;
            brpc::Controller c2;
            stub.CreateFile(&c2, &create_req, &st, nullptr);
            if (st.code() != 0) return nullptr;
            rpc::FindInodeReply find2;
            brpc::Controller c3;
            stub.FindInode(&c3, &req, &find2, nullptr);
            if (find2.status().code() != 0) return nullptr;
            return DeserializeInode(find2.inode());
        }
        auto inode = DeserializeInode(find_reply.inode());
        if ((flags & MO_TRUNC) && inode) {
            rpc::Status st;
            brpc::Controller c4;
            stub.TruncateFile(&c4, &req, &st, nullptr);
            if (st.code() != 0) return nullptr;
            brpc::Controller c5;
            rpc::FindInodeReply find3;
            stub.FindInode(&c5, &req, &find3, nullptr);
            if (find3.status().code() != 0) return nullptr;
            inode = DeserializeInode(find3.inode());
        }
        return inode;
    }

    int allocate_fd_locked(std::shared_ptr<Inode> inode, int flags) {
        int fd = acquire_fd_locked();
        if (fd < 0) return -1;
        fd_table_.emplace(fd, FdEntry{std::move(inode), 0, flags, 1});
        return fd;
    }

    int acquire_fd_locked() {
        size_t pos = fd_bitmap_.find_first();
        if (pos == boost::dynamic_bitset<>::npos) {
            size_t old = fd_bitmap_.size();
            fd_bitmap_.resize(std::max<size_t>(old * 2, 8), true);
            pos = fd_bitmap_.find_first();
            for (int fd : {0, 1, 2}) fd_bitmap_.reset(fd);
        } else if (pos < 3) {
            pos = fd_bitmap_.find_next(2);
        }
        if (pos == boost::dynamic_bitset<>::npos) return -1;
        fd_bitmap_.reset(pos);
        return static_cast<int>(pos);
    }

    void release_fd_locked(int fd) {
        if (fd >= 0 && static_cast<size_t>(fd) < fd_bitmap_.size()) {
            fd_bitmap_.set(static_cast<size_t>(fd));
        }
    }

    int shutdown_fd(int fd) {
        std::lock_guard<std::mutex> lk(fd_mutex_);
        auto it = fd_table_.find(fd);
        if (it == fd_table_.end()) return -1;
        if (--it->second.ref_count == 0) {
            fd_table_.erase(it);
            release_fd_locked(fd);
        }
        return 0;
    }

    off_t seek_fd(int fd, off_t offset, int whence) {
        std::lock_guard<std::mutex> lk(fd_mutex_);
        auto* entry = find_fd_locked(fd);
        if (!entry || !entry->inode) return -1;
        off_t base = 0;
        switch (whence) {
        case SEEK_SET: base = 0; break;
        case SEEK_CUR: base = static_cast<off_t>(entry->offset); break;
        case SEEK_END: base = static_cast<off_t>(entry->inode->getFileSize()); break;
        default: return -1;
        }
        off_t target = base + offset;
        if (target < 0) return -1;
        entry->offset = static_cast<size_t>(target);
        return target;
    }

    ssize_t write_fd(int fd, const char* buf, size_t count) {
        if (!buf || count == 0) return 0;
        std::shared_ptr<Inode> inode;
        size_t offset = 0;
        {
            std::lock_guard<std::mutex> lk(fd_mutex_);
            auto* entry = find_fd_locked(fd);
            if (!entry || !entry->inode) return -1;
            if (entry->flags & MO_RDONLY) return -1;
            offset = (entry->flags & MO_APPEND) ? entry->inode->getFileSize() : entry->offset;
            inode = entry->inode;
        }
        rpc::StorageService_Stub storage_stub(&storage_channel_);
        rpc::StorageIORequest req;
        SerializeInode(*inode, req.mutable_inode());
        req.set_offset(offset);
        req.set_data(buf, count);
        rpc::StorageWriteReply reply;
        brpc::Controller cntl;
        storage_stub.WriteFile(&cntl, &req, &reply, nullptr);
        if (reply.status().code() != 0) return reply.bytes();
        auto updated = DeserializeInode(reply.inode());
        if (!updated) return -1;

        {
            std::lock_guard<std::mutex> lk(fd_mutex_);
            auto* entry = find_fd_locked(fd);
            if (entry && entry->inode && entry->inode->inode == inode->inode) {
                entry->inode = updated;
                entry->offset = (entry->flags & MO_APPEND)
                    ? updated->getFileSize()
                    : entry->offset + static_cast<size_t>(reply.bytes());
            }
        }

        rpc::MdsService_Stub mds_stub(&mds_channel_);
        rpc::WriteInodeRequest win;
        win.set_ino(updated->inode);
        SerializeInode(*updated, win.mutable_inode());
        rpc::Status st;
        brpc::Controller c2;
        mds_stub.WriteInode(&c2, &win, &st, nullptr);
        (void)st;
        return reply.bytes();
    }

    ssize_t read_fd(int fd, char* buf, size_t count) {
        if (!buf || count == 0) return 0;
        std::shared_ptr<Inode> inode;
        size_t offset = 0;
        {
            std::lock_guard<std::mutex> lk(fd_mutex_);
            auto* entry = find_fd_locked(fd);
            if (!entry || !entry->inode) return -1;
            if (entry->flags & MO_WRONLY) return -1;
            offset = entry->offset;
            inode = entry->inode;
        }
        rpc::StorageService_Stub storage_stub(&storage_channel_);
        rpc::StorageIORequest req;
        SerializeInode(*inode, req.mutable_inode());
        req.set_offset(offset);
        req.set_data("", count); // only size matters for read
        rpc::StorageReadReply reply;
        brpc::Controller cntl;
        storage_stub.ReadFile(&cntl, &req, &reply, nullptr);
        if (reply.status().code() != 0) return reply.bytes();
        if (static_cast<size_t>(reply.bytes()) > count) return -1;
        memcpy(buf, reply.data().data(), static_cast<size_t>(reply.bytes()));
        auto updated = DeserializeInode(reply.inode());
        if (!updated) return -1;

        {
            std::lock_guard<std::mutex> lk(fd_mutex_);
            auto* entry = find_fd_locked(fd);
            if (entry && entry->inode && entry->inode->inode == inode->inode) {
                entry->inode = updated;
                entry->offset += static_cast<size_t>(reply.bytes());
            }
        }

        rpc::MdsService_Stub mds_stub(&mds_channel_);
        rpc::WriteInodeRequest win;
        win.set_ino(updated->inode);
        SerializeInode(*updated, win.mutable_inode());
        rpc::Status st;
        brpc::Controller c2;
        mds_stub.WriteInode(&c2, &win, &st, nullptr);
        (void)st;
        return reply.bytes();
    }

    FdEntry* find_fd_locked(int fd) {
        auto it = fd_table_.find(fd);
        return it == fd_table_.end() ? nullptr : &it->second;
    }

    void force_close_handles(uint64_t inode) {
        std::lock_guard<std::mutex> lk(fd_mutex_);
        for (auto it = fd_table_.begin(); it != fd_table_.end();) {
            if (it->second.inode && it->second.inode->inode == inode) {
                release_fd_locked(it->first);
                it = fd_table_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    friend class VfsMetricsHttpService;
    brpc::Channel mds_channel_;
    brpc::Channel storage_channel_;
    std::string mds_addr_;
    std::string storage_addr_;
    std::unordered_map<int, FdEntry> fd_table_;
    boost::dynamic_bitset<> fd_bitmap_;
    std::mutex fd_mutex_;
};

void VfsMetricsHttpService::default_method(google::protobuf::RpcController*,
                                           const brpc::HttpRequest*,
                                           brpc::HttpResponse* res,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server server;
    VfsServiceImpl svc(FLAGS_mds_addr, FLAGS_storage_addr);
    if (server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add vfs service" << std::endl;
        return -1;
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_vfs_idle_timeout;
    options.num_threads = FLAGS_vfs_thread_num;
    if (server.Start(FLAGS_vfs_port, &options) != 0) {
        std::cerr << "Failed to start vfs server" << std::endl;
        return -1;
    }
    std::cout << "VFS server listening on " << FLAGS_vfs_port << std::endl;
    server.RunUntilAskedToQuit();
    return 0;
}
