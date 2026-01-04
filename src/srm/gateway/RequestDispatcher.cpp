#include "RequestDispatcher.h"

#include <cerrno>
#include <cstring>
#include <iostream>

void RequestDispatcher::DispatchWrite(const storagenode::WriteRequest* req,
                                      storagenode::WriteReply* resp,
                                      brpc::Controller* cntl) {
    if (!req || !resp) {
        return;
    }
    if (req->node_id().empty()) {
        FillStatus(resp->mutable_status(), EINVAL, "missing node_id");
        return;
    }
    NodeContext ctx;
    if (!manager_ || !manager_->GetNode(req->node_id(), ctx)) {
        FillStatus(resp->mutable_status(), ENOENT, "unknown node");
        return;
    }
    if (ctx.type == NodeType::Virtual) {
        if (!virtual_engine_) {
            FillStatus(resp->mutable_status(), EIO, "virtual engine unavailable");
            return;
        }
        virtual_engine_->SimulateWrite(req, resp);
        return;
    }
    auto* stub = GetStub(ctx);
    if (!stub) {
        FillStatus(resp->mutable_status(), EIO, "failed to build channel");
        return;
    }
    storagenode::WriteReply tmp;
    storagenode::WriteReply* out = resp;
    brpc::Controller local_cntl;
    brpc::Controller* rpc_cntl = cntl ? cntl : &local_cntl;
    stub->Write(rpc_cntl, req, out, nullptr);
    if (rpc_cntl->Failed()) {
        FillStatus(out->mutable_status(), EIO, rpc_cntl->ErrorText());
    }
}

void RequestDispatcher::DispatchRead(const storagenode::ReadRequest* req,
                                     storagenode::ReadReply* resp,
                                     brpc::Controller* cntl) {
    if (!req || !resp) {
        return;
    }
    if (req->node_id().empty()) {
        FillStatus(resp->mutable_status(), EINVAL, "missing node_id");
        return;
    }
    NodeContext ctx;
    if (!manager_ || !manager_->GetNode(req->node_id(), ctx)) {
        FillStatus(resp->mutable_status(), ENOENT, "unknown node");
        return;
    }
    if (ctx.type == NodeType::Virtual) {
        if (!virtual_engine_) {
            FillStatus(resp->mutable_status(), EIO, "virtual engine unavailable");
            return;
        }
        virtual_engine_->SimulateRead(req, resp);
        return;
    }
    auto* stub = GetStub(ctx);
    if (!stub) {
        FillStatus(resp->mutable_status(), EIO, "failed to build channel");
        return;
    }
    storagenode::ReadReply tmp;
    storagenode::ReadReply* out = resp;
    brpc::Controller local_cntl;
    brpc::Controller* rpc_cntl = cntl ? cntl : &local_cntl;
    stub->Read(rpc_cntl, req, out, nullptr);
    if (rpc_cntl->Failed()) {
        FillStatus(out->mutable_status(), EIO, rpc_cntl->ErrorText());
    }
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

void RequestDispatcher::FillStatus(rpc::Status* status, int code, const std::string& msg) {
    if (!status) {
        return;
    }
    status->set_code(code);
    if (code == 0) {
        status->set_message("");
    } else {
        status->set_message(!msg.empty() ? msg : std::strerror(code));
    }
}
