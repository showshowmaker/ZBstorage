#include "RequestDispatcher.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <utility>

#include "common/StatusUtils.h"

namespace {

class RealNodeWriteCallback : public ::google::protobuf::Closure {
public:
    RealNodeWriteCallback(storagenode::WriteReply* client_resp,
                          ::google::protobuf::Closure* client_done,
                          std::unique_ptr<brpc::Controller> real_cntl,
                          std::unique_ptr<storagenode::WriteReply> real_resp)
        : client_resp_(client_resp),
          client_done_(client_done),
          real_cntl_(std::move(real_cntl)),
          real_resp_(std::move(real_resp)) {}

    brpc::Controller* controller() { return real_cntl_.get(); }
    storagenode::WriteReply* real_resp() { return real_resp_.get(); }

    void Run() override {
        if (!client_resp_) {
            delete this;
            return;
        }
        if (real_cntl_ && real_cntl_->Failed()) {
            StatusUtils::SetStatus(client_resp_->mutable_status(),
                                   rpc::STATUS_NETWORK_ERROR,
                                   real_cntl_->ErrorText());
        } else if (real_resp_) {
            client_resp_->set_bytes_written(real_resp_->bytes_written());
            client_resp_->mutable_status()->set_code(
                StatusUtils::NormalizeCode(real_resp_->status().code()));
            client_resp_->mutable_status()->set_message(real_resp_->status().message());
        } else {
            StatusUtils::SetStatus(client_resp_->mutable_status(),
                                   rpc::STATUS_UNKNOWN_ERROR,
                                   "empty response");
        }
        if (client_done_) client_done_->Run();
        delete this;
    }

private:
    storagenode::WriteReply* client_resp_{nullptr};
    ::google::protobuf::Closure* client_done_{nullptr};
    std::unique_ptr<brpc::Controller> real_cntl_;
    std::unique_ptr<storagenode::WriteReply> real_resp_;
};

class RealNodeReadCallback : public ::google::protobuf::Closure {
public:
    RealNodeReadCallback(storagenode::ReadReply* client_resp,
                         ::google::protobuf::Closure* client_done,
                         std::unique_ptr<brpc::Controller> real_cntl,
                         std::unique_ptr<storagenode::ReadReply> real_resp)
        : client_resp_(client_resp),
          client_done_(client_done),
          real_cntl_(std::move(real_cntl)),
          real_resp_(std::move(real_resp)) {}

    brpc::Controller* controller() { return real_cntl_.get(); }
    storagenode::ReadReply* real_resp() { return real_resp_.get(); }

    void Run() override {
        if (!client_resp_) {
            delete this;
            return;
        }
        if (real_cntl_ && real_cntl_->Failed()) {
            StatusUtils::SetStatus(client_resp_->mutable_status(),
                                   rpc::STATUS_NETWORK_ERROR,
                                   real_cntl_->ErrorText());
        } else if (real_resp_) {
            client_resp_->set_bytes_read(real_resp_->bytes_read());
            client_resp_->mutable_data()->swap(*real_resp_->mutable_data());
            client_resp_->set_checksum(real_resp_->checksum());
            client_resp_->mutable_status()->set_code(
                StatusUtils::NormalizeCode(real_resp_->status().code()));
            client_resp_->mutable_status()->set_message(real_resp_->status().message());
        } else {
            StatusUtils::SetStatus(client_resp_->mutable_status(),
                                   rpc::STATUS_UNKNOWN_ERROR,
                                   "empty response");
        }
        if (client_done_) client_done_->Run();
        delete this;
    }

private:
    storagenode::ReadReply* client_resp_{nullptr};
    ::google::protobuf::Closure* client_done_{nullptr};
    std::unique_ptr<brpc::Controller> real_cntl_;
    std::unique_ptr<storagenode::ReadReply> real_resp_;
};

} // namespace

void RequestDispatcher::DispatchWrite(const storagenode::WriteRequest* req,
                                      storagenode::WriteReply* resp,
                                      brpc::Controller*,
                                      ::google::protobuf::Closure* done) {
    if (!req || !resp) {
        if (done) done->Run();
        return;
    }
    if (req->node_id().empty()) {
        FillStatus(resp->mutable_status(), rpc::STATUS_INVALID_ARGUMENT, "missing node_id");
        if (done) done->Run();
        return;
    }
    NodeContext ctx;
    if (!manager_ || !manager_->GetNode(req->node_id(), ctx)) {
        FillStatus(resp->mutable_status(), rpc::STATUS_NODE_NOT_FOUND, "unknown node");
        if (done) done->Run();
        return;
    }
    if (ctx.type == NodeType::Virtual) {
        if (!virtual_engine_) {
            FillStatus(resp->mutable_status(), rpc::STATUS_VIRTUAL_NODE_ERROR, "virtual engine unavailable");
            if (done) done->Run();
            return;
        }
        virtual_engine_->SimulateWrite(req, resp);
        if (done) done->Run();
        return;
    }
    auto* stub = GetStub(ctx);
    if (!stub) {
        FillStatus(resp->mutable_status(), rpc::STATUS_NETWORK_ERROR, "failed to build channel");
        if (done) done->Run();
        return;
    }
    auto real_cntl = std::make_unique<brpc::Controller>();
    auto real_resp = std::make_unique<storagenode::WriteReply>();
    auto* callback = new RealNodeWriteCallback(resp, done, std::move(real_cntl), std::move(real_resp));
    stub->Write(callback->controller(), req, callback->real_resp(), callback);
    // Ownership of callback and internal state handled within callback.
}

void RequestDispatcher::DispatchRead(const storagenode::ReadRequest* req,
                                     storagenode::ReadReply* resp,
                                     brpc::Controller*,
                                     ::google::protobuf::Closure* done) {
    if (!req || !resp) {
        if (done) done->Run();
        return;
    }
    if (req->node_id().empty()) {
        FillStatus(resp->mutable_status(), rpc::STATUS_INVALID_ARGUMENT, "missing node_id");
        if (done) done->Run();
        return;
    }
    NodeContext ctx;
    if (!manager_ || !manager_->GetNode(req->node_id(), ctx)) {
        FillStatus(resp->mutable_status(), rpc::STATUS_NODE_NOT_FOUND, "unknown node");
        if (done) done->Run();
        return;
    }
    if (ctx.type == NodeType::Virtual) {
        if (!virtual_engine_) {
            FillStatus(resp->mutable_status(), rpc::STATUS_VIRTUAL_NODE_ERROR, "virtual engine unavailable");
            if (done) done->Run();
            return;
        }
        virtual_engine_->SimulateRead(req, resp);
        if (done) done->Run();
        return;
    }
    auto* stub = GetStub(ctx);
    if (!stub) {
        FillStatus(resp->mutable_status(), rpc::STATUS_NETWORK_ERROR, "failed to build channel");
        if (done) done->Run();
        return;
    }
    auto real_cntl = std::make_unique<brpc::Controller>();
    auto real_resp = std::make_unique<storagenode::ReadReply>();
    auto* callback = new RealNodeReadCallback(resp, done, std::move(real_cntl), std::move(real_resp));
    stub->Read(callback->controller(), req, callback->real_resp(), callback);
}

storagenode::StorageService_Stub* RequestDispatcher::GetStub(const NodeContext& ctx) {
    const std::string key = ctx.node_id;
    std::lock_guard<std::mutex> lk(stub_mu_);
    auto it = stubs_.find(key);
    if (it != stubs_.end()) {
        return it->second.stub.get();
    }
    auto entry = StubEntry{};
    entry.channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions opts;
    const std::string addr = ctx.ip + ":" + std::to_string(ctx.port);
    if (entry.channel->Init(addr.c_str(), &opts) != 0) {
        std::cerr << "[Gateway] failed to init channel to " << addr << std::endl;
        return nullptr;
    }
    entry.stub = std::make_unique<storagenode::StorageService_Stub>(entry.channel.get());
    auto* stub_ptr = entry.stub.get();
    stubs_.emplace(key, std::move(entry));
    return stub_ptr;
}

void RequestDispatcher::FillStatus(rpc::Status* status, rpc::StatusCode code, const std::string& msg) {
    StatusUtils::SetStatus(status, code, msg);
}
