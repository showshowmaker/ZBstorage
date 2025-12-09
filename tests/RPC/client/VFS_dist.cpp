// src/client/VFS_dist.cpp
#include "client/VFS_dist.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

#include "proto/common.pb.h"

using fs::common::StatusCode;

DistFileSystem::DistFileSystem(const std::string& mds_addr) {
    brpc::ChannelOptions options;
    if (mds_channel_.Init(mds_addr.c_str(), &options) != 0) {
        throw std::runtime_error("init mds channel failed: " + mds_addr);
    }
    mds_stub_ = std::make_unique<fs::mds::MdsService_Stub>(&mds_channel_);
}

int DistFileSystem::open(const std::string& path, int flags, int mode) {
    uint64_t inode = 0;
    bool need_create = false;

#ifdef MO_CREAT
    need_create = (flags & MO_CREAT) != 0;
#else
    need_create = (flags & O_CREAT) != 0;
#endif

    // 1. 先 Lookup
    {
        fs::mds::LookupRequest req;
        fs::mds::LookupResponse resp;
        brpc::Controller cntl;

        req.set_path(path);
        mds_stub_->Lookup(&cntl, &req, &resp, nullptr);

        if (!cntl.Failed() && resp.status() == StatusCode::OK) {
            inode = resp.inode_id();
        } else if (resp.status() == StatusCode::NOT_FOUND && need_create) {
            // 2. 不存在且需要创建 -> CreateFile
            fs::mds::CreateFileRequest creq;
            fs::mds::CreateFileResponse cresp;
            brpc::Controller cc;

            creq.set_path(path);
            creq.set_mode(mode);
            mds_stub_->CreateFile(&cc, &creq, &cresp, nullptr);
            if (cc.Failed() || cresp.status() != StatusCode::OK) {
                std::cerr << "[client] CreateFile failed, path=" << path
                          << " error=" << cc.ErrorText() << "\n";
                return -1;
            }
            inode = cresp.inode_id();
        } else {
            std::cerr << "[client] Lookup failed, path=" << path
                      << " err=" << cntl.ErrorText() << "\n";
            return -1;
        }
    }

    std::lock_guard<std::mutex> lk(mu_);
    int fd = next_fd_++;
    fd_table_[fd] = FileHandle{inode, 0};
    return fd;
}

ssize_t DistFileSystem::write(int fd, const void* buf, size_t count) {
    FileHandle fh;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_table_.find(fd);
        if (it == fd_table_.end()) return -1;
        fh = it->second;
    }

    // 1. 询问 MDS：这个 inode 在当前 offset 应该写到哪里
    fs::mds::GetBlockInfoRequest req;
    fs::mds::GetBlockInfoResponse resp;
    brpc::Controller cntl;

    req.set_inode_id(fh.inode_id);
    req.set_file_offset(fh.offset);
    req.set_is_write(true);

    mds_stub_->GetBlockInfo(&cntl, &req, &resp, nullptr);
    if (cntl.Failed() || resp.status() != StatusCode::OK) {
        std::cerr << "[client] GetBlockInfo(write) failed: " << cntl.ErrorText() << "\n";
        return -1;
    }

    // 2. 向对应 Storage 发起 Write
    brpc::Channel* storage_ch = GetOrCreateStorageChannel(resp.storage_addr());
    if (!storage_ch) return -1;

    fs::storage::StorageService_Stub storage_stub(storage_ch);
    fs::storage::WriteRequest sreq;
    fs::storage::WriteResponse sresp;
    brpc::Controller scntl;

    sreq.set_volume_id(resp.volume_id());
    sreq.set_offset(resp.block_offset());
    sreq.set_data(buf, count);

    storage_stub.Write(&scntl, &sreq, &sresp, nullptr);
    if (scntl.Failed() || sresp.status() != StatusCode::OK) {
        std::cerr << "[client] storage Write failed: " << scntl.ErrorText() << "\n";
        return -1;
    }

    // 3. 更新本地 offset
    ssize_t n = sresp.bytes_written();
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_table_.find(fd);
        if (it != fd_table_.end()) {
            it->second.offset += static_cast<uint64_t>(n);
        }
    }
    return n;
}

ssize_t DistFileSystem::read(int fd, void* buf, size_t count) {
    FileHandle fh;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_table_.find(fd);
        if (it == fd_table_.end()) return -1;
        fh = it->second;
    }

    fs::mds::GetBlockInfoRequest req;
    fs::mds::GetBlockInfoResponse resp;
    brpc::Controller cntl;

    req.set_inode_id(fh.inode_id);
    req.set_file_offset(fh.offset);
    req.set_is_write(false);

    mds_stub_->GetBlockInfo(&cntl, &req, &resp, nullptr);
    if (cntl.Failed() || resp.status() != StatusCode::OK) {
        std::cerr << "[client] GetBlockInfo(read) failed: " << cntl.ErrorText() << "\n";
        return -1;
    }

    brpc::Channel* storage_ch = GetOrCreateStorageChannel(resp.storage_addr());
    if (!storage_ch) return -1;

    fs::storage::StorageService_Stub storage_stub(storage_ch);
    fs::storage::ReadRequest sreq;
    fs::storage::ReadResponse sresp;
    brpc::Controller scntl;

    sreq.set_volume_id(resp.volume_id());
    sreq.set_offset(resp.block_offset());
    sreq.set_length(static_cast<int32_t>(count));

    storage_stub.Read(&scntl, &sreq, &sresp, nullptr);
    if (scntl.Failed() || sresp.status() != StatusCode::OK) {
        std::cerr << "[client] storage Read failed: " << scntl.ErrorText() << "\n";
        return -1;
    }

    const std::string& data = sresp.data();
    size_t n = std::min(data.size(), count);
    std::memcpy(buf, data.data(), n);

    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_table_.find(fd);
        if (it != fd_table_.end()) {
            it->second.offset += static_cast<uint64_t>(n);
        }
    }
    return static_cast<ssize_t>(n);
}

off_t DistFileSystem::seek(int fd, off_t offset, int whence) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = fd_table_.find(fd);
    if (it == fd_table_.end()) return -1;

    uint64_t base = 0;
    if (whence == SEEK_SET) {
        base = 0;
    } else if (whence == SEEK_CUR) {
        base = it->second.offset;
    } else {
        // SEEK_END 需要文件大小，目前我们没有从 MDS 获取 size，这里简单不支持
        return -1;
    }

    int64_t new_off = static_cast<int64_t>(base) + offset;
    if (new_off < 0) return -1;

    it->second.offset = static_cast<uint64_t>(new_off);
    return static_cast<off_t>(new_off);
}

int DistFileSystem::close(int fd) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = fd_table_.find(fd);
    if (it == fd_table_.end()) return -1;
    fd_table_.erase(it);
    return 0;
}

brpc::Channel* DistFileSystem::GetOrCreateStorageChannel(const std::string& addr) {
    auto it = storage_channels_.find(addr);
    if (it != storage_channels_.end()) {
        return it->second.get();
    }

    auto ch = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    if (ch->Init(addr.c_str(), &options) != 0) {
        std::cerr << "[client] init storage channel " << addr << " failed\n";
        return nullptr;
    }
    brpc::Channel* raw = ch.get();
    storage_channels_.emplace(addr, std::move(ch));
    return raw;
}
