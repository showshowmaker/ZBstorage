#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cluster_manager.pb.h"
#include "NodeRegistry.h"
#include <brpc/channel.h>
#include "mds.pb.h"
#include "fs/volume/Volume.h"
#include "fs/block/BlockManager.h"
#include "fs/volume/VolumeRegistry.h"

class StorageNodeManager {
public:
    enum class VolumePolicy {
        PreferReal,
        PreferVirtual,
        AllSsd,
    };

    StorageNodeManager(std::chrono::milliseconds heartbeat_timeout,
                       std::chrono::milliseconds health_check_interval,
                       std::string mds_addr,
                       VolumePolicy policy);
    ~StorageNodeManager();

    StorageNodeManager(const StorageNodeManager&) = delete;
    StorageNodeManager& operator=(const StorageNodeManager&) = delete;

    void Start();
    void Stop();

    void HandleRegister(const storagenode::RegisterRequest* request,
                        storagenode::RegisterResponse* response);

    void HandleHeartbeat(const storagenode::HeartbeatRequest* request,
                         storagenode::HeartbeatResponse* response);

    bool GetNode(const std::string& node_id, NodeContext& ctx) const;
    // Optional: pre-register a virtual node with simulation parameters.
    void AddVirtualNode(const std::string& node_id, const SimulationParams& params, uint64_t capacity_bytes = 0);

private:
    void HealthLoop();
    std::string GenerateNodeId();
    void RegisterToMDS(const NodeContext& ctx);
    VolumeType ResolveVolumeType(const NodeContext& ctx) const;

    NodeRegistry registry_;
    std::atomic<uint64_t> id_seq_{1};
    std::chrono::milliseconds heartbeat_timeout_;
    std::chrono::milliseconds health_check_interval_;
    std::atomic<bool> running_{false};
    std::thread health_thread_;
    std::string mds_addr_;
    std::unique_ptr<brpc::Channel> mds_channel_;
    std::unique_ptr<rpc::MdsService_Stub> mds_stub_;
    VolumePolicy volume_policy_{VolumePolicy::PreferReal};
};
