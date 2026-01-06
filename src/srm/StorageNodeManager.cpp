#include "StorageNodeManager.h"

#include <brpc/channel.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <utility>

#include "rpc_common.pb.h"
#include "common/StatusUtils.h"

StorageNodeManager::StorageNodeManager(std::chrono::milliseconds heartbeat_timeout,
                                       std::chrono::milliseconds health_check_interval,
                                       std::string mds_addr,
                                       VolumePolicy policy)
    : heartbeat_timeout_(heartbeat_timeout),
      health_check_interval_(health_check_interval),
      mds_addr_(std::move(mds_addr)),
      volume_policy_(policy) {}

StorageNodeManager::~StorageNodeManager() {
    Stop();
}

void StorageNodeManager::Start() {
    if (running_.exchange(true)) {
        return;
    }
    if (!mds_addr_.empty()) {
        mds_channel_ = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions opts;
        opts.protocol = "baidu_std";
        opts.timeout_ms = 2000;
        opts.max_retry = 1;
        if (mds_channel_->Init(mds_addr_.c_str(), &opts) != 0) {
            mds_channel_.reset();
        } else {
            mds_stub_ = std::make_unique<rpc::MdsService_Stub>(mds_channel_.get());
        }
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
        StatusUtils::SetStatus(response->mutable_status(), rpc::STATUS_INVALID_ARGUMENT, "missing ip/port");
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
    StatusUtils::SetStatus(response->mutable_status(), rpc::STATUS_SUCCESS, "");
    NodeContext added;
    if (registry_.Get(response->node_id(), added)) {
        RegisterToMDS(added);
    }
}

void StorageNodeManager::HandleHeartbeat(const storagenode::HeartbeatRequest* request,
                                         storagenode::HeartbeatResponse* response) {
    if (!request || !response) {
        return;
    }
    if (request->node_id().empty()) {
        StatusUtils::SetStatus(response->mutable_status(), rpc::STATUS_INVALID_ARGUMENT, "empty node_id");
        response->set_require_rereg(true);
        return;
    }
    bool ok = registry_.UpdateHeartbeat(request->node_id(),
                                        std::chrono::steady_clock::now());
    if (!ok) {
        StatusUtils::SetStatus(response->mutable_status(), rpc::STATUS_NODE_NOT_FOUND, "node not registered");
        response->set_require_rereg(true);
        return;
    }
    StatusUtils::SetStatus(response->mutable_status(), rpc::STATUS_SUCCESS, "");
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

bool StorageNodeManager::GetNode(const std::string& node_id, NodeContext& ctx) const {
    return registry_.Get(node_id, ctx);
}

void StorageNodeManager::AddVirtualNode(const std::string& node_id, const SimulationParams& params, uint64_t capacity_bytes) {
    NodeContext ctx;
    ctx.node_id = node_id;
    ctx.type = NodeType::Virtual;
    ctx.sim_params = params;
    ctx.state = NodeState::Online;
    ctx.last_heartbeat = std::chrono::steady_clock::now();
    if (capacity_bytes > 0) {
        storagenode::DiskInfo d;
        d.set_mount_point("/virtual");
        d.set_total_bytes(capacity_bytes);
        d.set_free_bytes(capacity_bytes);
        ctx.disks.push_back(d);
    }
    registry_.Upsert(std::move(ctx));
    NodeContext added;
    if (registry_.Get(node_id, added)) {
        RegisterToMDS(added);
    }
}

void StorageNodeManager::RegisterToMDS(const NodeContext& ctx) {
    if (!mds_stub_) {
        return;
    }
    uint64_t capacity_bytes = 100ULL * 1024 * 1024 * 1024; // default 100GB
    if (!ctx.disks.empty() && ctx.disks[0].total_bytes() > 0) {
        capacity_bytes = ctx.disks[0].total_bytes();
    }
    uint64_t total_blocks = capacity_bytes / 4096;

    const std::string volume_path = "/virtual"; // placeholder path
    const size_t block_size = 4096;
    const size_t blocks_per_group = 64;
    auto vol = std::make_shared<Volume>(ctx.node_id, volume_path, total_blocks, block_size, blocks_per_group);

    auto data = vol->serialize();

    rpc::RegisterVolumeRequest req;
    req.mutable_volume()->set_data(data.data(), data.size());
    req.set_type(static_cast<uint32_t>(ResolveVolumeType(ctx)));
    req.set_persist_now(false);

    rpc::RegisterVolumeReply resp;
    brpc::Controller cntl;
    mds_stub_->RegisterVolume(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[SRM] RegisterVolume to MDS failed for node " << ctx.node_id
                  << ": " << cntl.ErrorText() << std::endl;
    } else if (resp.status().code() != 0) {
        std::cerr << "[SRM] RegisterVolume rejected for node " << ctx.node_id
                  << " code=" << resp.status().code() << " msg=" << resp.status().message() << std::endl;
    } else {
        std::cerr << "[SRM] Synced node " << ctx.node_id << " to MDS volume index "
                  << resp.index() << std::endl;
    }
}

VolumeType StorageNodeManager::ResolveVolumeType(const NodeContext& ctx) const {
    switch (volume_policy_) {
        case VolumePolicy::PreferReal:
            return (ctx.type == NodeType::Virtual) ? VolumeType::HDD : VolumeType::SSD;
        case VolumePolicy::PreferVirtual:
            return (ctx.type == NodeType::Virtual) ? VolumeType::SSD : VolumeType::HDD;
        case VolumePolicy::AllSsd:
        default:
            return VolumeType::SSD;
    }
}
