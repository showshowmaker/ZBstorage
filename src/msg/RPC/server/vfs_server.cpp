#include "vfs_server.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include "../../../src/mds/inode/InodeTimestamp.h"

namespace rpc {

RpcVfsServer::RpcVfsServer(std::shared_ptr<RpcMdsClient> mds_client,
                           std::shared_ptr<RpcStorageClient> storage_client)
    : mds_(std::move(mds_client)),
      storage_(std::move(storage_client)) {
    // Pre-reserve fd bitmap with standard fds marked unavailable.
    if (fd_bitmap_.empty()) {
        fd_bitmap_.resize(4096, true);
        for (int fd : {0, 1, 2}) {
            if (fd < static_cast<int>(fd_bitmap_.size())) {
                fd_bitmap_.reset(fd);
            }
        }
    }
}

Status RpcVfsServer::Startup() {
    if (!mds_) return Status::Error(1, "mds client missing");
    auto st = mds_->CreateRoot();
    return st;
}

Status RpcVfsServer::Shutdown() {
    // Only registry flush happens on MDS side, nothing special here.
    return Status::Ok();
}

bool RpcVfsServer::CreateRootDirectory() {
    return mds_ && mds_->CreateRoot().ok();
}

bool RpcVfsServer::Mkdir(const std::string& path, mode_t mode) {
    return mds_ && mds_->Mkdir(path, mode).ok();
}

bool RpcVfsServer::Rmdir(const std::string& path) {
    return mds_ && mds_->Rmdir(path).ok();
}

bool RpcVfsServer::Ls(const std::string& path) {
    DirectoryList list;
    if (!mds_) return false;
    auto st = mds_->Ls(path, list);
    if (!st.ok()) return false;
    for (const auto& entry : list.entries) {
        std::string name(entry.name, entry.name + entry.name_len);
        std::cout << name << std::endl;
    }
    return true;
}

uint64_t RpcVfsServer::LookupInode(const std::string& path) {
    return mds_ ? mds_->LookupIno(path) : static_cast<uint64_t>(-1);
}

bool RpcVfsServer::CreateFile(const std::string& path, mode_t mode) {
    return mds_ && mds_->CreateFile(path, mode).ok();
}

bool RpcVfsServer::RemoveFile(const std::string& path) {
    if (!mds_) return false;
    auto reply = mds_->RemoveFile(path);
    for (auto ino : reply.detached_inodes) {
        force_close_handles(ino);
    }
    return reply.status.ok();
}

bool RpcVfsServer::RegisterVolume(const std::shared_ptr<Volume>& vol,
                                  VolumeType type,
                                  int* out_index,
                                  bool persist_now) {
    if (!vol) return false;
    bool ok = true;
    if (storage_) {
        ok = storage_->RegisterVolume(vol, type).ok();
    }
    if (mds_) {
        auto st = mds_->RegisterVolume(vol, type, out_index, persist_now);
        ok = ok && st.ok();
    } else {
        ok = false;
    }
    return ok;
}

int RpcVfsServer::acquire_fd_locked() {
    size_t pos = fd_bitmap_.find_first();
    if (pos == boost::dynamic_bitset<>::npos) {
        const size_t old_size = fd_bitmap_.size();
        const size_t new_size = std::max<size_t>(old_size ? old_size * 2 : 8, 8);
        fd_bitmap_.resize(new_size, true);
        pos = fd_bitmap_.find_first();
        for (int fd : {0, 1, 2}) {
            if (fd < static_cast<int>(fd_bitmap_.size())) {
                fd_bitmap_.reset(fd);
            }
        }
    } else if (pos < 3) {
        pos = fd_bitmap_.find_next(2);
    }
    if (pos == boost::dynamic_bitset<>::npos) return -1;
    fd_bitmap_.reset(pos);
    return static_cast<int>(pos);
}

void RpcVfsServer::release_fd_locked(int fd) {
    if (fd >= 0 && static_cast<size_t>(fd) < fd_bitmap_.size()) {
        fd_bitmap_.set(static_cast<size_t>(fd));
    }
}

FdTableEntry* RpcVfsServer::find_fd_locked(int fd) {
    auto it = fd_table_.find(fd);
    return it == fd_table_.end() ? nullptr : &it->second;
}

int RpcVfsServer::allocate_fd_locked(std::shared_ptr<Inode> inode, int flags) {
    int fd = acquire_fd_locked();
    if (fd < 0) return -1;
    fd_table_.emplace(fd, FdTableEntry(std::move(inode), flags));
    return fd;
}

int RpcVfsServer::Open(const std::string& path, int flags, mode_t mode) {
    if (!mds_) return -1;

    std::shared_ptr<Inode> inode = mds_->FindInode(path);
    if (!inode) {
        if (!(flags & MO_CREAT)) return -1;
        if (!mds_->CreateFile(path, mode).ok()) return -1;
        inode = mds_->FindInode(path);
    } else if ((flags & MO_TRUNC) && !mds_->TruncateFile(path).ok()) {
        return -1;
    }
    if (!inode) return -1;

    std::lock_guard lk(fd_mutex_);
    return allocate_fd_locked(std::move(inode), flags);
}

int RpcVfsServer::Close(int fd) {
    return ShutdownFd(fd);
}

int RpcVfsServer::ShutdownFd(int fd) {
    std::lock_guard lk(fd_mutex_);
    auto it = fd_table_.find(fd);
    if (it == fd_table_.end()) return -1;
    if (--it->second.ref_count <= 0) {
        fd_table_.erase(it);
        release_fd_locked(fd);
    }
    return 0;
}

off_t RpcVfsServer::Seek(int fd, off_t offset, int whence) {
    std::lock_guard lk(fd_mutex_);
    FdTableEntry* entry = find_fd_locked(fd);
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

ssize_t RpcVfsServer::Write(int fd, const char* buf, size_t count) {
    if (count == 0) return 0;
    if (!buf || !mds_ || !storage_) return -1;

    std::shared_ptr<Inode> inode;
    size_t offset = 0;
    {
        std::lock_guard lk(fd_mutex_);
        FdTableEntry* entry = find_fd_locked(fd);
        if (!entry || !entry->inode) return -1;
        if (entry->flags & MO_RDONLY) return -1;
        offset = (entry->flags & MO_APPEND) ? entry->inode->getFileSize() : entry->offset;
        inode = entry->inode;
    }

    auto reply = storage_->WriteFile(inode, offset, buf, count);
    if (!reply.status.ok()) return reply.bytes;

    {
        std::lock_guard lk(fd_mutex_);
        FdTableEntry* entry = find_fd_locked(fd);
        if (entry && entry->inode && entry->inode->inode == inode->inode) {
            entry->inode = reply.inode;
            entry->offset = (entry->flags & MO_APPEND)
                ? reply.inode->getFileSize()
                : entry->offset + static_cast<size_t>(reply.bytes);
        }
    }

    InodeTimestamp now;
    reply.inode->setFmTime(now);
    reply.inode->setFaTime(now);
    reply.inode->setFcTime(now);
    mds_->WriteInode(reply.inode->inode, *reply.inode);
    return reply.bytes;
}

ssize_t RpcVfsServer::Read(int fd, char* buf, size_t count) {
    if (count == 0) return 0;
    if (!buf || !mds_ || !storage_) return -1;

    std::shared_ptr<Inode> inode;
    size_t offset = 0;
    {
        std::lock_guard lk(fd_mutex_);
        FdTableEntry* entry = find_fd_locked(fd);
        if (!entry || !entry->inode) return -1;
        if (entry->flags & MO_WRONLY) return -1;
        offset = entry->offset;
        inode = entry->inode;
    }

    auto reply = storage_->ReadFile(inode, offset, count);
    if (!reply.status.ok()) return reply.bytes;
    if (static_cast<size_t>(reply.bytes) > count) return -1;
    std::copy(reply.data.begin(), reply.data.end(), buf);

    {
        std::lock_guard lk(fd_mutex_);
        FdTableEntry* entry = find_fd_locked(fd);
        if (entry && entry->inode && entry->inode->inode == inode->inode) {
            entry->inode = reply.inode;
            entry->offset += static_cast<size_t>(reply.bytes);
        }
    }

    InodeTimestamp now;
    reply.inode->setFaTime(now);
    mds_->WriteInode(reply.inode->inode, *reply.inode);
    return reply.bytes;
}

std::vector<uint64_t> RpcVfsServer::CollectColdInodes(size_t max_candidates, size_t min_age_windows) {
    return mds_ ? mds_->CollectColdInodes(max_candidates, min_age_windows).inodes
                : std::vector<uint64_t>{};
}

std::shared_ptr<boost::dynamic_bitset<>> RpcVfsServer::CollectColdInodesBitmap(size_t min_age_windows) {
    return mds_ ? mds_->CollectColdInodesBitmap(min_age_windows).bitmap
                : nullptr;
}

std::vector<uint64_t> RpcVfsServer::CollectColdInodesByAtimePercent(double percent) {
    return mds_ ? mds_->CollectColdInodesByPercent(percent).inodes
                : std::vector<uint64_t>{};
}

void RpcVfsServer::force_close_handles(uint64_t inode) {
    std::lock_guard lk(fd_mutex_);
    for (auto it = fd_table_.begin(); it != fd_table_.end();) {
        if (it->second.inode && it->second.inode->inode == inode) {
            release_fd_locked(it->first);
            it = fd_table_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace rpc

#ifndef RPC_BUILD_LIBRARY_ONLY
int main() {
    // Standalone build target placeholder to satisfy tests/CMakeLists globbed executables.
    return 0;
}
#endif
