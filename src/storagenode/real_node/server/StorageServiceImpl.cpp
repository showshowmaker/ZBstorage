#include "StorageServiceImpl.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

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
                                       std::shared_ptr<IOEngine> io_engine)
    : disk_manager_(std::move(disk_manager)), io_engine_(std::move(io_engine)) {
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
    auto res = io_engine_->Write(request->chunk_id(),
                                 request->data().data(),
                                 request->data().size(),
                                 request->offset());
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
    if (!io_engine_) {
        FillStatus(status, EINVAL, "io engine is null");
        return;
    }

    std::string buffer;
    auto res = io_engine_->Read(request->chunk_id(),
                                request->offset(),
                                static_cast<size_t>(request->length()),
                                buffer);
    if (res.bytes < 0 || res.err != 0) {
        int err = res.err != 0 ? res.err : EIO;
        FillStatus(status, err, res.err != 0 ? "" : "read failed");
        return;
    }
    response->set_bytes_read(static_cast<uint64_t>(res.bytes));
    response->set_data(buffer.data(), static_cast<size_t>(res.bytes));
    response->set_checksum(ComputeChecksum(buffer.data(), static_cast<size_t>(res.bytes)));
    Ok(status);
}

uint64_t StorageServiceImpl::ComputeChecksum(const void* data, size_t len) const {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint64_t hash = 1469598103934665603ULL;      // FNV offset basis
    const uint64_t prime = 1099511628211ULL;      // FNV prime
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<uint64_t>(bytes[i]);
        hash *= prime;
    }
    return hash;
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
