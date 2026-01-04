#include "DfsClient.h"

#include <brpc/controller.h>
#include <cstring>
#include <unistd.h>

#include "msg/RPC/proto/mds.pb.h"
#include "msg/RPC/proto/vfs.pb.h"
#include "mds/inode/inode.h"

DfsClient::DfsClient(MountConfig cfg)
    : cfg_(std::move(cfg)), rpc_(std::make_unique<RpcClients>(cfg_)) {}

bool DfsClient::Init() {
    return rpc_->Init();
}

int DfsClient::StatusToErrno(rpc::StatusCode code) const {
    switch (code) {
        case rpc::STATUS_SUCCESS: return 0;
        case rpc::STATUS_INVALID_ARGUMENT: return EINVAL;
        case rpc::STATUS_NODE_NOT_FOUND: return ENOENT;
        case rpc::STATUS_IO_ERROR: return EIO;
        case rpc::STATUS_NETWORK_ERROR: return ECOMM;
        default: return EIO;
    }
}

bool DfsClient::PopulateStatFromInode(const rpc::FindInodeReply& reply, struct stat* st) const {
    if (!st) return false;
    if (!reply.has_inode()) return false;
    const auto& blob = reply.inode().data();
    Inode inode;
    size_t offset = 0;
    if (!Inode::deserialize(reinterpret_cast<const uint8_t*>(blob.data()), offset, inode, blob.size())) {
        return false;
    }
    std::memset(st, 0, sizeof(struct stat));
    st->st_mode = inode.file_mode.raw;
    st->st_size = static_cast<off_t>(inode.getFileSize());
    st->st_nlink = 1;
    st->st_uid = 0;
    st->st_gid = 0;
    st->st_atime = 0;
    st->st_mtime = 0;
    st->st_ctime = 0;
    return true;
}

int DfsClient::GetAttr(const std::string& path, struct stat* st) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathRequest req;
    rpc::FindInodeReply resp;
    brpc::Controller cntl;
    req.set_path(path);
    rpc_->mds()->FindInode(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) {
        return -StatusToErrno(code);
    }
    if (!PopulateStatFromInode(resp, st)) {
        return -EIO;
    }
    return 0;
}

int DfsClient::ReadDir(const std::string& path, void* buf, fuse_fill_dir_t filler) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathRequest req;
    rpc::DirectoryListReply resp;
    brpc::Controller cntl;
    req.set_path(path);
    rpc_->mds()->Ls(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) return -ECOMM;
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) return -StatusToErrno(code);

    filler(buf, ".", nullptr, 0);
    filler(buf, "..", nullptr, 0);
    for (const auto& entry : resp.entries()) {
        filler(buf, entry.name().c_str(), nullptr, 0);
    }
    return 0;
}

int DfsClient::Open(const std::string& path, int flags, int& out_fd) {
    if (!rpc_ || !rpc_->vfs()) return -ECOMM;
    rpc::OpenRequest req;
    rpc::IOReplyFD resp;
    brpc::Controller cntl;
    req.set_path(path);
    req.set_flags(flags);
    req.set_mode(0644);
    rpc_->vfs()->Open(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) return -ECOMM;
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) return -StatusToErrno(code);
    out_fd = static_cast<int>(resp.bytes());
    return 0;
}

int DfsClient::Create(const std::string& path, int flags, mode_t mode, int& out_fd) {
    if (!rpc_ || !rpc_->mds() || !rpc_->vfs()) return -ECOMM;
    rpc::PathModeRequest creq;
    rpc::Status cresp;
    brpc::Controller ccntl;
    creq.set_path(path);
    creq.set_mode(static_cast<uint32_t>(mode));
    rpc_->mds()->CreateFile(&ccntl, &creq, &cresp, nullptr);
    if (ccntl.Failed()) return -ECOMM;
    auto ccode = StatusUtils::NormalizeCode(cresp.code());
    if (ccode != rpc::STATUS_SUCCESS) return -StatusToErrno(ccode);
    return Open(path, flags, out_fd);
}

int DfsClient::Read(int fd, char* buf, size_t size, off_t offset, ssize_t& out_bytes) {
    if (!rpc_ || !rpc_->vfs()) return -ECOMM;
    // Seek to desired offset
    rpc::SeekRequest seek_req;
    rpc::SeekReply seek_resp;
    brpc::Controller seek_cntl;
    seek_req.set_fd(fd);
    seek_req.set_offset(offset);
    seek_req.set_whence(SEEK_SET);
    rpc_->vfs()->Seek(&seek_cntl, &seek_req, &seek_resp, nullptr);
    if (seek_cntl.Failed()) return -ECOMM;
    auto seek_code = StatusUtils::NormalizeCode(seek_resp.status().code());
    if (seek_code != rpc::STATUS_SUCCESS) return -StatusToErrno(seek_code);

    rpc::IORequestFD req;
    rpc::IOReplyFD resp;
    brpc::Controller cntl;
    req.set_fd(fd);
    req.set_data(std::string(size, '\0')); // length hint
    rpc_->vfs()->Read(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) return -ECOMM;
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) return -StatusToErrno(code);
    out_bytes = resp.bytes();
    if (out_bytes > 0 && static_cast<size_t>(out_bytes) <= size) {
        std::memcpy(buf, resp.data().data(), static_cast<size_t>(out_bytes));
    }
    return 0;
}

int DfsClient::Write(int fd, const char* buf, size_t size, off_t offset, ssize_t& out_bytes) {
    if (!rpc_ || !rpc_->vfs()) return -ECOMM;
    // Seek to desired offset
    rpc::SeekRequest seek_req;
    rpc::SeekReply seek_resp;
    brpc::Controller seek_cntl;
    seek_req.set_fd(fd);
    seek_req.set_offset(offset);
    seek_req.set_whence(SEEK_SET);
    rpc_->vfs()->Seek(&seek_cntl, &seek_req, &seek_resp, nullptr);
    if (seek_cntl.Failed()) return -ECOMM;
    auto seek_code = StatusUtils::NormalizeCode(seek_resp.status().code());
    if (seek_code != rpc::STATUS_SUCCESS) return -StatusToErrno(seek_code);

    rpc::IORequestFD req;
    rpc::IOReplyFD resp;
    brpc::Controller cntl;
    req.set_fd(fd);
    req.set_data(buf, size);
    rpc_->vfs()->Write(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) return -ECOMM;
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) return -StatusToErrno(code);
    out_bytes = resp.bytes();
    return 0;
}

int DfsClient::Close(int fd) {
    if (!rpc_ || !rpc_->vfs()) return -ECOMM;
    rpc::FdRequest req;
    rpc::Status resp;
    brpc::Controller cntl;
    req.set_fd(fd);
    rpc_->vfs()->Close(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) return -ECOMM;
    auto code = StatusUtils::NormalizeCode(resp.code());
    if (code != rpc::STATUS_SUCCESS) return -StatusToErrno(code);
    return 0;
}
