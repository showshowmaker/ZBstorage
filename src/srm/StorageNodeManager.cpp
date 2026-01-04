#include "StorageNodeManager.h"

#include <brpc/channel.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <utility>

StorageNodeManager::StorageNodeManager(std::chrono::milliseconds heartbeat_timeout,
                                       std::chrono::milliseconds health_check_interval)
    : heartbeat_timeout_(heartbeat_timeout),
      health_check_interval_(health_check_interval) {}

StorageNodeManager::~StorageNodeManager() {
    Stop();
}

void StorageNodeManager::Start() {
    if (running_.exchange(true)) {
        return;
    }
    health_thread_ = std::thread([this]() { HealthLoop(); });
}

void StorageNodeManager::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (health_thread_.joinable()) {
        health_thread_.join();
    }
}

void StorageNodeManager::HandleRegister(const storagenode::RegisterRequest* request,
                                        storagenode::RegisterResponse* response) {
    if (!request || !response) {
        return;
    }
    if (request->ip().empty() || request->port() == 0) {
        FillStatus(response->mutable_status(), EINVAL, "missing ip/port");
        return;
    }

    NodeContext ctx;
    ctx.node_id = GenerateNodeId();
    ctx.ip = request->ip();
    ctx.port = request->port();
    ctx.hostname = request->hostname();
    ctx.disks.assign(request->disks().begin(), request->disks().end());
    ctx.type = NodeType::Real;
    ctx.state = NodeState::Online;
    ctx.last_heartbeat = std::chrono::steady_clock::now();

    registry_.Upsert(std::move(ctx));
    response->set_node_id(ctx.node_id);
    FillStatus(response->mutable_status(), 0, "");
    std::cerr << "[SRM] node registered id=" << ctx.node_id << " ip=" << request->ip()
              << ":" << request->port() << " disks=" << request->disks_size() << std::endl;
}

void StorageNodeManager::HandleHeartbeat(const storagenode::HeartbeatRequest* request,
                                         storagenode::HeartbeatResponse* response) {
    if (!request || !response) {
        return;
    }
    if (request->node_id().empty()) {
        FillStatus(response->mutable_status(), EINVAL, "empty node_id");
        response->set_require_rereg(true);
        return;
    }
    bool ok = registry_.UpdateHeartbeat(request->node_id(),
                                        std::chrono::steady_clock::now());
    if (!ok) {
        FillStatus(response->mutable_status(), ENOENT, "node not registered");
        response->set_require_rereg(true);
        return;
    }
    FillStatus(response->mutable_status(), 0, "");
    response->set_require_rereg(false);
}

void StorageNodeManager::HealthLoop() {
    while (running_) {
        const auto now = std::chrono::steady_clock::now();
        auto snapshot = registry_.Snapshot();
        for (const auto& ctx : snapshot) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - ctx.last_heartbeat);
            if (elapsed > heartbeat_timeout_ && ctx.state != NodeState::Offline) {
                registry_.MarkOffline(ctx.node_id);
                std::cerr << "[SRM] node offline id=" << ctx.node_id
                          << " elapsed_ms=" << elapsed.count() << std::endl;
            }
        }
        std::this_thread::sleep_for(health_check_interval_);
    }
}

std::string StorageNodeManager::GenerateNodeId() {
    uint64_t seq = id_seq_.fetch_add(1);
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return "node-" + std::to_string(now) + "-" + std::to_string(seq);
}

void StorageNodeManager::FillStatus(rpc::Status* status, int code, const std::string& msg) {
    if (!status) {
        return;
    }
    status->set_code(code);
    if (code == 0) {
        status->set_message("");
        return;
    }
    if (!msg.empty()) {
        status->set_message(msg);
    } else {
        status->set_message(std::strerror(code));
    }
}

bool StorageNodeManager::GetNode(const std::string& node_id, NodeContext& ctx) const {
    return registry_.Get(node_id, ctx);
}

void StorageNodeManager::AddVirtualNode(const std::string& node_id, const SimulationParams& params) {
    NodeContext ctx;
    ctx.node_id = node_id;
    ctx.type = NodeType::Virtual;
    ctx.sim_params = params;
    ctx.state = NodeState::Online;
    ctx.last_heartbeat = std::chrono::steady_clock::now();
    registry_.Upsert(std::move(ctx));
}
