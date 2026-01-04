#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "cluster_manager.pb.h"
#include "NodeRegistry.h"

class StorageNodeManager {
public:
    StorageNodeManager(std::chrono::milliseconds heartbeat_timeout,
                       std::chrono::milliseconds health_check_interval);
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
    void AddVirtualNode(const std::string& node_id, const SimulationParams& params);

private:
    void HealthLoop();
    std::string GenerateNodeId();
    void FillStatus(rpc::Status* status, int code, const std::string& msg);

    NodeRegistry registry_;
    std::atomic<uint64_t> id_seq_{1};
    std::chrono::milliseconds heartbeat_timeout_;
    std::chrono::milliseconds health_check_interval_;
    std::atomic<bool> running_{false};
    std::thread health_thread_;
};
