#include "NodeRegistry.h"

#include <utility>

void NodeRegistry::Upsert(NodeContext ctx) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    ctx.last_heartbeat = std::chrono::steady_clock::now();
    nodes_[ctx.node_id] = std::move(ctx);
}

bool NodeRegistry::UpdateHeartbeat(const std::string& node_id,
                                   std::chrono::steady_clock::time_point now) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    it->second.last_heartbeat = now;
    it->second.state = NodeState::Online;
    return true;
}

bool NodeRegistry::MarkOffline(const std::string& node_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    it->second.state = NodeState::Offline;
    return true;
}

bool NodeRegistry::Exists(const std::string& node_id) const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    return nodes_.find(node_id) != nodes_.end();
}

bool NodeRegistry::Get(const std::string& node_id, NodeContext& out) const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    out = it->second;
    return true;
}

std::vector<NodeContext> NodeRegistry::Snapshot() const {
    std::vector<NodeContext> out;
    std::shared_lock<std::shared_mutex> lk(mu_);
    out.reserve(nodes_.size());
    for (const auto& kv : nodes_) {
        out.push_back(kv.second);
    }
    return out;
}
