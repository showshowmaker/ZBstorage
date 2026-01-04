#pragma once

#include <chrono>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cluster_manager.pb.h"

enum class NodeState {
    Online,
    Offline,
    Suspected,
};

enum class NodeType {
    Real,
    Virtual,
};

struct SimulationParams {
    int min_latency_ms{5};
    int max_latency_ms{50};
    double failure_rate{0.0};
};

struct NodeContext {
    std::string node_id;
    std::string ip;
    uint32_t port{0};
    std::string hostname;
    std::vector<storagenode::DiskInfo> disks;
    NodeType type{NodeType::Real};
    SimulationParams sim_params{};
    NodeState state{NodeState::Online};
    std::chrono::steady_clock::time_point last_heartbeat;
};

class NodeRegistry {
public:
    NodeRegistry() = default;

    void Upsert(NodeContext ctx);
    bool UpdateHeartbeat(const std::string& node_id, std::chrono::steady_clock::time_point now);
    bool MarkOffline(const std::string& node_id);
    bool Exists(const std::string& node_id) const;
    bool Get(const std::string& node_id, NodeContext& out) const;
    std::vector<NodeContext> Snapshot() const;

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, NodeContext> nodes_;
};
