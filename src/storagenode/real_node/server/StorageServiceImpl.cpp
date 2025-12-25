#include "StorageServiceImpl.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <butil/crc32c.h>

#include <fcntl.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

namespace {

rpc::Status* Ok(rpc::Status* status) {
    status->set_code(0);
    status->set_message("");
    return status;
}

} // namespace

StorageServiceImpl::StorageServiceImpl(std::shared_ptr<DiskManager> disk_manager,
                                       std::shared_ptr<LocalMetadataManager> metadata_mgr,
                                       std::shared_ptr<IOEngine> io_engine)
    : disk_manager_(std::move(disk_manager)),
      metadata_mgr_(std::move(metadata_mgr)),
      io_engine_(std::move(io_engine)) {
    if (disk_manager_) {
        ready_ = disk_manager_->Prepare();
    } else {
        ready_ = true;
    }
}

void StorageServiceImpl::Write(::google::protobuf::RpcController* controller,
                               const storagenode::WriteRequest* request,
                               storagenode::WriteReply* response,
                               ::google::protobuf::Closure* done) {
    (void)controller;
    brpc::ClosureGuard guard(done);
    auto* status = response->mutable_status();
    if (!ready_) {
        FillStatus(status, EIO, "disk not ready");
        return;
    }
    if (!metadata_mgr_) {
        FillStatus(status, EINVAL, "metadata manager is null");
        return;
    }
    if (!io_engine_) {
        FillStatus(status, EINVAL, "io engine is null");
        return;
    }
    if (request->checksum() != 0) {
        uint64_t actual = ComputeChecksum(request->data().data(), request->data().size());
        if (actual != request->checksum()) {
            FillStatus(status, EINVAL, "payload checksum mismatch");
            return;
        }
    }
    std::string path = metadata_mgr_->GetPath(request->chunk_id());
    if (path.empty()) {
        path = metadata_mgr_->AllocPath(request->chunk_id());
        if (path.empty()) {
            FillStatus(status, EIO, "failed to allocate path");
            return;
        }
    }

    int flags = request->flags();
    if (flags == 0) {
        flags = O_WRONLY | O_CREAT;
    }
    if ((flags & (O_WRONLY | O_RDWR)) == 0) {
        flags |= O_WRONLY;
    }
    int mode = request->mode() == 0 ? 0644 : request->mode();

    auto res = io_engine_->Write(path,
                                 request->data().data(),
                                 request->data().size(),
                                 request->offset(),
                                 flags,
                                 mode);
    if (res.bytes < 0 || res.err != 0) {
        int err = res.err != 0 ? res.err : EIO;
        FillStatus(status, err, res.err != 0 ? "" : "write failed");
        return;
    }
    response->set_bytes_written(static_cast<uint64_t>(res.bytes));
    Ok(status);
}

void StorageServiceImpl::Read(::google::protobuf::RpcController* controller,
                              const storagenode::ReadRequest* request,
                              storagenode::ReadReply* response,
                              ::google::protobuf::Closure* done) {
    (void)controller;
    brpc::ClosureGuard guard(done);
    auto* status = response->mutable_status();
    if (!ready_) {
        FillStatus(status, EIO, "disk not ready");
        return;
    }
    if (!metadata_mgr_) {
        FillStatus(status, EINVAL, "metadata manager is null");
        return;
    }
    if (!io_engine_) {
        FillStatus(status, EINVAL, "io engine is null");
        return;
    }

    std::string path = metadata_mgr_->GetPath(request->chunk_id());
    if (path.empty()) {
        FillStatus(status, ENOENT, "chunk not found");
        return;
    }

    int flags = request->flags();
    if (flags == 0) {
        flags = O_RDONLY;
    }
    // ensure read-only if caller passed write flags accidentally
    flags &= ~(O_WRONLY | O_RDWR | O_CREAT | O_TRUNC | O_EXCL);
    if ((flags & (O_RDONLY | O_WRONLY | O_RDWR)) == 0) {
        flags |= O_RDONLY;
    }

    std::string buffer;
    auto res = io_engine_->Read(path,
                                request->offset(),
                                static_cast<size_t>(request->length()),
                                buffer,
                                flags);
    if (res.bytes < 0 || res.err != 0) {
        int err = res.err != 0 ? res.err : EIO;
        FillStatus(status, err, res.err != 0 ? "" : "read failed");
        return;
    }
    response->set_bytes_read(static_cast<uint64_t>(res.bytes));
    // avoid extra copy inside protobuf by swapping buffers
    response->mutable_data()->swap(buffer);
    response->set_checksum(ComputeChecksum(response->data().data(),
                                           static_cast<size_t>(res.bytes)));
    Ok(status);
}

uint64_t StorageServiceImpl::ComputeChecksum(const void* data, size_t len) const {
    return butil::crc32c::Value(static_cast<const char*>(data), len);
}

void StorageServiceImpl::FillStatus(rpc::Status* status, int err, const std::string& message) const {
    if (!status) {
        return;
    }
    status->set_code(err);
    if (err == 0) {
        status->set_message("");
        return;
    }
    if (!message.empty()) {
        status->set_message(message);
    } else {
        status->set_message(std::strerror(err));
    }
}
