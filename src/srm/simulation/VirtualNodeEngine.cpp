#include "VirtualNodeEngine.h"

#include <bthread/bthread.h>
#include <butil/crc32c.h>

#include <chrono>
#include <thread>
#include <cerrno>

VirtualNodeEngine::VirtualNodeEngine(SimulationConfig cfg)
    : cfg_(cfg),
      rng_(static_cast<uint32_t>(
          std::chrono::steady_clock::now().time_since_epoch().count())),
      latency_dist_(cfg_.min_latency_ms, cfg_.max_latency_ms),
      failure_dist_(0.0, 1.0) {}

void VirtualNodeEngine::SimulateWrite(const storagenode::WriteRequest* req,
                                      storagenode::WriteReply* resp) {
    if (!resp) {
        return;
    }
    MaybeFail(resp->mutable_status());
    if (resp->status().code() != 0) {
        return;
    }
    AddLatency();
    // Compute checksum to mimic work
    (void)butil::crc32c::Value(req->data().data(), req->data().size());
    resp->set_bytes_written(static_cast<uint64_t>(req->data().size()));
    FillStatus(resp->mutable_status(), 0, "");
}

void VirtualNodeEngine::SimulateRead(const storagenode::ReadRequest* req,
                                     storagenode::ReadReply* resp) {
    if (!resp) {
        return;
    }
    MaybeFail(resp->mutable_status());
    if (resp->status().code() != 0) {
        return;
    }
    AddLatency();
    uint64_t len = req && req->length() > 0 ? req->length() : cfg_.default_read_size;
    std::string data(static_cast<size_t>(len), '\0');
    resp->mutable_data()->swap(data);
    resp->set_bytes_read(len);
    resp->set_checksum(butil::crc32c::Value(resp->data().data(),
                                            static_cast<size_t>(len)));
    FillStatus(resp->mutable_status(), 0, "");
}

void VirtualNodeEngine::MaybeFail(rpc::Status* status) {
    if (!status) {
        return;
    }
    if (failure_dist_(rng_) < cfg_.failure_rate) {
        FillStatus(status, EIO, "simulated failure");
    }
}

void VirtualNodeEngine::AddLatency() {
    int ms = latency_dist_(rng_);
    bthread_usleep(static_cast<useconds_t>(ms) * 1000);
}

void VirtualNodeEngine::FillStatus(rpc::Status* status, int code, const std::string& msg) {
    status->set_code(code);
    if (code == 0) {
        status->set_message("");
        return;
    }
    status->set_message(msg.empty() ? "error" : msg);
}
