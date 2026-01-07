#include "DfsClient.h"

#include <brpc/controller.h>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "mds.pb.h"
#include "storage_node.pb.h"

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

bool DfsClient::PopulateStat(struct stat* st, bool is_dir) const {
    if (!st) return false;
    std::memset(st, 0, sizeof(struct stat));
    st->st_mode = (is_dir ? (S_IFDIR | 0755) : (S_IFREG | 0644));
    st->st_size = 0;
    st->st_nlink = is_dir ? 2 : 1;
    st->st_uid = 0;
    st->st_gid = 0;
    st->st_atime = 0;
    st->st_mtime = 0;
    st->st_ctime = 0;
    return true;
}

rpc::StatusCode DfsClient::LookupInode(const std::string& path, InodeInfo& out_info) {
    if (!rpc_ || !rpc_->mds()) return rpc::STATUS_NETWORK_ERROR;
    rpc::PathRequest req;
    rpc::FindInodeReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_path(path);
    rpc_->mds()->FindInode(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] FindInode failed path=" << path << " err=" << cntl.ErrorText() << std::endl;
        return rpc::STATUS_NETWORK_ERROR;
    }
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] FindInode failed path=" << path
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.status().message() << std::endl;
        return code;
    }
    out_info.inode = 0;
    // For safety, also call LookupIno to get inode number.
    rpc::LookupReply lresp;
    brpc::Controller lcntl;
    lcntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    rpc_->mds()->LookupIno(&lcntl, &req, &lresp, nullptr);
    if (!lcntl.Failed()) {
        auto lcode = StatusUtils::NormalizeCode(lresp.status().code());
        if (lcode == rpc::STATUS_SUCCESS) {
            out_info.inode = lresp.inode();
        } else {
            std::cerr << "[Client] LookupIno failed path=" << path << " code=" << static_cast<int>(lcode) << std::endl;
            return lcode;
        }
    } else {
        std::cerr << "[Client] LookupIno RPC failed path=" << path << " err=" << lcntl.ErrorText() << std::endl;
        return rpc::STATUS_NETWORK_ERROR;
    }
    out_info.node_id = !resp.node_id().empty() ? resp.node_id() : resp.volume_id();
    return rpc::STATUS_SUCCESS;
}

rpc::StatusCode DfsClient::UpdateRemoteSize(uint64_t inode, uint64_t size_bytes) {
    if (!rpc_ || !rpc_->mds()) return rpc::STATUS_NETWORK_ERROR;
    rpc::UpdateFileSizeRequest req;
    rpc::Status resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_inode(inode);
    req.set_size_bytes(size_bytes);
    rpc_->mds()->UpdateFileSize(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] UpdateFileSize RPC failed inode=" << inode
                  << " err=" << cntl.ErrorText() << std::endl;
        return rpc::STATUS_NETWORK_ERROR;
    }
    return StatusUtils::NormalizeCode(resp.code());
}

int DfsClient::GetAttr(const std::string& path, struct stat* st) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    InodeInfo info;
    auto code = LookupInode(path, info);
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Open LookupInode failed path=" << path
                  << " code=" << static_cast<int>(code) << std::endl;
        return -StatusToErrno(code);
    }
    bool is_dir = (path == "/");
    if (!is_dir) {
        rpc::PathRequest lreq;
        rpc::DirectoryListReply lresp;
        brpc::Controller lcntl;
        lcntl.set_timeout_ms(cfg_.rpc_timeout_ms);
        lreq.set_path(path);
        rpc_->mds()->Ls(&lcntl, &lreq, &lresp, nullptr);
        if (!lcntl.Failed()) {
            auto lcode = StatusUtils::NormalizeCode(lresp.status().code());
            if (lcode == rpc::STATUS_SUCCESS) {
                is_dir = true;
            }
        }
    }
    if (!PopulateStat(st, is_dir)) return -EIO;
    if (!is_dir) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = inode_size_.find(info.inode);
        if (it != inode_size_.end()) {
            st->st_size = static_cast<off_t>(it->second);
        }
    }
    return 0;
}

int DfsClient::ReadDir(const std::string& path, void* buf, fuse_fill_dir_t filler) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathRequest req;
    rpc::DirectoryListReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_path(path);
    rpc_->mds()->Ls(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] ReadDir RPC failed path=" << path << " err=" << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
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
    InodeInfo info;
    auto code = LookupInode(path, info);
    if (code != rpc::STATUS_SUCCESS) {
        if (code == rpc::STATUS_NODE_NOT_FOUND && (flags & O_CREAT) != 0) {
            return Create(path, flags, 0644, out_fd);
        }
        return -StatusToErrno(code);
    }
    int fd = next_fd_++;
    {
        std::lock_guard<std::mutex> lk(mu_);
        fd_info_[fd] = info;
        if (inode_size_.find(info.inode) == inode_size_.end()) {
            inode_size_[info.inode] = 0;
        }
    }
    out_fd = fd;
    return 0;
}

int DfsClient::Create(const std::string& path, int flags, mode_t mode, int& out_fd) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathModeRequest creq;
    rpc::Status cresp;
    brpc::Controller ccntl;
    ccntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    creq.set_path(path);
    creq.set_mode(static_cast<uint32_t>(mode));
    rpc_->mds()->CreateFile(&ccntl, &creq, &cresp, nullptr);
    if (ccntl.Failed()) {
        std::cerr << "[Client] CreateFile RPC failed path=" << path << " err=" << ccntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto ccode = StatusUtils::NormalizeCode(cresp.code());
    if (ccode != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] CreateFile failed path=" << path
                  << " code=" << static_cast<int>(ccode)
                  << " msg=" << cresp.message() << std::endl;
        return -StatusToErrno(ccode);
    }
    return Open(path, flags, out_fd);
}

int DfsClient::Mkdir(const std::string& path, mode_t mode) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathModeRequest req;
    rpc::Status resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_path(path);
    req.set_mode(static_cast<uint32_t>(mode));
    rpc_->mds()->Mkdir(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] Mkdir RPC failed path=" << path << " err=" << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Mkdir failed path=" << path
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.message() << std::endl;
        return -StatusToErrno(code);
    }
    return 0;
}

int DfsClient::Rmdir(const std::string& path) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    rpc::PathRequest req;
    rpc::Status resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_path(path);
    rpc_->mds()->Rmdir(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] Rmdir RPC failed path=" << path << " err=" << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Rmdir failed path=" << path
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.message() << std::endl;
        return -StatusToErrno(code);
    }
    return 0;
}

int DfsClient::Unlink(const std::string& path) {
    if (!rpc_ || !rpc_->mds()) return -ECOMM;
    InodeInfo info;
    const bool had_inode = (LookupInode(path, info) == rpc::STATUS_SUCCESS);
    rpc::PathRequest req;
    rpc::RemoveFileReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    req.set_path(path);
    rpc_->mds()->RemoveFile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] Unlink RPC failed path=" << path << " err=" << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Unlink failed path=" << path
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.status().message() << std::endl;
        return -StatusToErrno(code);
    }
    if (had_inode) {
        std::lock_guard<std::mutex> lk(mu_);
        inode_size_.erase(info.inode);
    }
    return 0;
}

int DfsClient::Truncate(const std::string& path, off_t size) {
    if (!rpc_ || !rpc_->mds() || !rpc_->srm()) return -ECOMM;
    if (size < 0) return -EINVAL;

    InodeInfo info;
    auto code = LookupInode(path, info);
    if (code != rpc::STATUS_SUCCESS) {
        return -StatusToErrno(code);
    }

    storagenode::TruncateRequest treq;
    storagenode::TruncateReply tresp;
    brpc::Controller tcntl;
    tcntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    const std::string& node_id = info.node_id.empty() ? cfg_.default_node_id : info.node_id;
    treq.set_node_id(node_id);
    treq.set_chunk_id(static_cast<uint64_t>(info.inode));
    treq.set_size(static_cast<uint64_t>(size));
    rpc_->srm()->Truncate(&tcntl, &treq, &tresp, nullptr);
    if (tcntl.Failed()) {
        std::cerr << "[Client] Truncate SRM RPC failed path=" << path
                  << " err=" << tcntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto tcode = StatusUtils::NormalizeCode(tresp.status().code());
    if (tcode != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Truncate failed path=" << path
                  << " code=" << static_cast<int>(tcode)
                  << " msg=" << tresp.status().message() << std::endl;
        return -StatusToErrno(tcode);
    }

    auto ucode = UpdateRemoteSize(info.inode, static_cast<uint64_t>(size));
    if (ucode != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] UpdateFileSize failed inode=" << info.inode
                  << " code=" << static_cast<int>(ucode) << std::endl;
        return -StatusToErrno(ucode);
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        inode_size_[info.inode] = static_cast<uint64_t>(size);
    }
    return 0;
}

int DfsClient::Read(int fd, char* buf, size_t size, off_t offset, ssize_t& out_bytes) {
    if (!rpc_ || !rpc_->srm()) return -ECOMM;
    InodeInfo info;
    uint64_t known_size = 0;
    bool has_size = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_info_.find(fd);
        if (it == fd_info_.end()) return -EBADF;
        info = it->second;
        auto sit = inode_size_.find(info.inode);
        if (sit != inode_size_.end()) {
            known_size = sit->second;
            has_size = true;
        }
    }
    if (has_size && offset >= static_cast<off_t>(known_size)) {
        out_bytes = 0;
        return 0;
    }
    size_t req_len = size;
    if (has_size) {
        uint64_t remain = known_size - static_cast<uint64_t>(offset);
        req_len = static_cast<size_t>(std::min<uint64_t>(remain, size));
    }

    storagenode::ReadRequest req;
    storagenode::ReadReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    const std::string& node_id = info.node_id.empty() ? cfg_.default_node_id : info.node_id;
    req.set_node_id(node_id);
    req.set_chunk_id(static_cast<uint64_t>(info.inode));
    req.set_offset(static_cast<uint64_t>(offset));
    req.set_length(static_cast<uint64_t>(req_len));
    rpc_->srm()->Read(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] Read RPC failed: " << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Read failed fd=" << fd
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.status().message() << std::endl;
        return -StatusToErrno(code);
    }
    out_bytes = static_cast<ssize_t>(resp.bytes_read());
    if (out_bytes > 0 && static_cast<size_t>(out_bytes) <= size) {
        std::memcpy(buf, resp.data().data(), static_cast<size_t>(out_bytes));
    }
    return 0;
}

int DfsClient::Write(int fd, const char* buf, size_t size, off_t offset, ssize_t& out_bytes) {
    if (!rpc_ || !rpc_->srm()) return -ECOMM;
    InodeInfo info;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_info_.find(fd);
        if (it == fd_info_.end()) return -EBADF;
        info = it->second;
    }

    storagenode::WriteRequest req;
    storagenode::WriteReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(cfg_.rpc_timeout_ms);
    const std::string& node_id = info.node_id.empty() ? cfg_.default_node_id : info.node_id;
    req.set_node_id(node_id);
    req.set_chunk_id(static_cast<uint64_t>(info.inode));
    req.set_offset(static_cast<uint64_t>(offset));
    req.set_data(buf, size);
    req.set_checksum(0);
    req.set_flags(0);
    req.set_mode(0644);

    rpc_->srm()->Write(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[Client] Write RPC failed: " << cntl.ErrorText() << std::endl;
        return -ECOMM;
    }
    auto code = StatusUtils::NormalizeCode(resp.status().code());
    if (code != rpc::STATUS_SUCCESS) {
        std::cerr << "[Client] Write failed fd=" << fd
                  << " code=" << static_cast<int>(code)
                  << " msg=" << resp.status().message() << std::endl;
        return -StatusToErrno(code);
    }
    out_bytes = static_cast<ssize_t>(resp.bytes_written());
    uint64_t new_size = 0;
    bool need_update = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
        uint64_t end = static_cast<uint64_t>(offset) + static_cast<uint64_t>(out_bytes);
        auto& cur = inode_size_[info.inode];
        if (end > cur) {
            cur = end;
            new_size = cur;
            need_update = true;
        }
    }
    if (need_update) {
        auto code = UpdateRemoteSize(info.inode, new_size);
        if (code != rpc::STATUS_SUCCESS) {
            std::cerr << "[Client] UpdateFileSize failed inode=" << info.inode
                      << " code=" << static_cast<int>(code) << std::endl;
            return -StatusToErrno(code);
        }
    }
    return 0;
}

int DfsClient::Close(int fd) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = fd_info_.find(fd);
    if (it != fd_info_.end()) {
        fd_info_.erase(it);
    }
    return 0;
}
