#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "mds/inode/inode.h"

class VirtualNodeLedger {
public:
    struct NodeSummary {
        std::string node_id;
        int type{0};
        uint64_t total_bytes{0};
        uint64_t free_bytes{0};
    };

    bool LoadFromJson(const std::string& path, std::string* err);
    bool SnapshotToJson(const std::string& path) const;
    bool InitEmpty(uint32_t ssd_nodes, uint32_t hdd_nodes, uint32_t mix_nodes, uint64_t capacity_bytes);

    std::vector<NodeSummary> ListNodes() const;
    bool GetNodeCapacity(const std::string& node_id, uint64_t* total_bytes, uint64_t* free_bytes) const;

    // Apply inode allocation; returns resolved node_id when success.
    bool ApplyInode(const Inode& inode, std::string* out_node_id);

    bool TakeDirty();

private:
    struct DeviceState {
        std::string device_id;
        std::string type;
        double read_MBps{0.0};
        double write_MBps{0.0};
        uint64_t capacity{0};
        uint64_t used{0};
        uint64_t free{0};
    };

    struct NodeState {
        std::string node_id;
        int type{0};
        std::vector<DeviceState> ssd_devices;
        std::vector<DeviceState> hdd_devices;
    };

    std::string ResolveNodeId(uint16_t idx, uint8_t type) const;
    bool Allocate(NodeState& node, uint64_t bytes);
    void RebuildIndex();

    mutable std::mutex mu_;
    std::unordered_map<std::string, NodeState> nodes_;
    std::vector<std::string> ssd_nodes_;
    std::vector<std::string> hdd_nodes_;
    std::vector<std::string> mix_nodes_;
    bool dirty_{false};
};
