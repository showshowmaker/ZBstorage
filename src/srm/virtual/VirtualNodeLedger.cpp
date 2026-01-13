#include "virtual/VirtualNodeLedger.h"

#include <algorithm>
#include <fstream>
#include <limits>

#include <nlohmann/json.hpp>

namespace {

bool ParseIndex(const std::string& value, const std::string& prefix, uint16_t* out) {
    if (value.rfind(prefix, 0) != 0) {
        return false;
    }
    const std::string suffix = value.substr(prefix.size());
    if (suffix.empty()) {
        return false;
    }
    for (char c : suffix) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    try {
        unsigned long idx = std::stoul(suffix);
        if (idx > std::numeric_limits<uint16_t>::max()) {
            return false;
        }
        *out = static_cast<uint16_t>(idx);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

bool VirtualNodeLedger::LoadFromJson(const std::string& path, std::string* err) {
    std::ifstream in(path);
    if (!in.is_open()) {
        if (err) {
            *err = "failed to open json: " + path;
        }
        return false;
    }
    nlohmann::json root;
    try {
        in >> root;
    } catch (const std::exception& ex) {
        if (err) {
            *err = ex.what();
        }
        return false;
    }
    if (!root.contains("nodes") || !root["nodes"].is_array()) {
        if (err) {
            *err = "missing nodes array";
        }
        return false;
    }

    std::unordered_map<std::string, NodeState> next;
    auto load_devices = [](const nlohmann::json& list, std::vector<DeviceState>* out) {
        if (!list.is_array()) {
            return;
        }
        for (const auto& dev_j : list) {
            DeviceState dev;
            dev.device_id = dev_j.value("device_id", "");
            dev.type = dev_j.value("type", "");
            dev.read_MBps = dev_j.value("read_throughput_MBps", 0.0);
            dev.write_MBps = dev_j.value("write_throughput_MBps", 0.0);
            dev.capacity = dev_j.value("capacity", 0ULL);
            if (dev_j.contains("used_bytes")) {
                dev.used = dev_j.value("used_bytes", 0ULL);
            } else if (dev_j.contains("used")) {
                dev.used = dev_j.value("used", 0ULL);
            }
            if (dev_j.contains("free_bytes")) {
                dev.free = dev_j.value("free_bytes", 0ULL);
            } else if (dev_j.contains("free")) {
                dev.free = dev_j.value("free", 0ULL);
            } else {
                dev.free = dev.capacity > dev.used ? dev.capacity - dev.used : 0ULL;
            }
            if (dev.free > dev.capacity) {
                dev.free = dev.capacity;
            }
            if (dev.used > dev.capacity) {
                dev.used = dev.capacity;
            }
            out->push_back(std::move(dev));
        }
    };
    for (const auto& node_j : root["nodes"]) {
        NodeState node;
        node.node_id = node_j.value("node_id", "");
        if (node.node_id.empty()) {
            continue;
        }
        node.type = node_j.value("type", 0);
        if (node_j.contains("ssd_devices")) {
            load_devices(node_j["ssd_devices"], &node.ssd_devices);
        }
        if (node_j.contains("hdd_devices")) {
            load_devices(node_j["hdd_devices"], &node.hdd_devices);
        }
        next[node.node_id] = std::move(node);
    }

    {
        std::lock_guard<std::mutex> lk(mu_);
        nodes_ = std::move(next);
        RebuildIndex();
        dirty_ = false;
    }
    return true;
}

bool VirtualNodeLedger::SnapshotToJson(const std::string& path) const {
    nlohmann::json root;
    root["nodes"] = nlohmann::json::array();

    std::vector<NodeState> nodes;
    {
        std::lock_guard<std::mutex> lk(mu_);
        nodes.reserve(nodes_.size());
        for (const auto& kv : nodes_) {
            nodes.push_back(kv.second);
        }
    }
    std::sort(nodes.begin(), nodes.end(),
              [](const NodeState& a, const NodeState& b) { return a.node_id < b.node_id; });

    for (const auto& node : nodes) {
        nlohmann::json node_j;
        node_j["node_id"] = node.node_id;
        node_j["type"] = node.type;
        node_j["ssd_devices"] = nlohmann::json::array();
        for (const auto& dev : node.ssd_devices) {
            nlohmann::json dev_j;
            dev_j["device_id"] = dev.device_id;
            dev_j["type"] = dev.type;
            dev_j["capacity"] = dev.capacity;
            dev_j["read_throughput_MBps"] = dev.read_MBps;
            dev_j["write_throughput_MBps"] = dev.write_MBps;
            dev_j["used_bytes"] = dev.used;
            dev_j["free_bytes"] = dev.free;
            node_j["ssd_devices"].push_back(std::move(dev_j));
        }
        node_j["ssd_device_count"] = static_cast<int>(node.ssd_devices.size());
        node_j["hdd_devices"] = nlohmann::json::array();
        for (const auto& dev : node.hdd_devices) {
            nlohmann::json dev_j;
            dev_j["device_id"] = dev.device_id;
            dev_j["type"] = dev.type;
            dev_j["capacity"] = dev.capacity;
            dev_j["read_throughput_MBps"] = dev.read_MBps;
            dev_j["write_throughput_MBps"] = dev.write_MBps;
            dev_j["used_bytes"] = dev.used;
            dev_j["free_bytes"] = dev.free;
            node_j["hdd_devices"].push_back(std::move(dev_j));
        }
        node_j["hdd_device_count"] = static_cast<int>(node.hdd_devices.size());
        root["nodes"].push_back(std::move(node_j));
    }

    std::ofstream out(path, std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }
    out << root.dump(4);
    return true;
}

bool VirtualNodeLedger::InitEmpty(uint32_t ssd_nodes, uint32_t hdd_nodes, uint32_t mix_nodes, uint64_t capacity_bytes) {
    std::unordered_map<std::string, NodeState> next;

    for (uint32_t i = 0; i < ssd_nodes; ++i) {
        NodeState node;
        node.node_id = "node_ssd_" + std::to_string(i);
        node.type = 0;
        DeviceState dev;
        dev.device_id = node.node_id + "_SSD_0";
        dev.type = "SolidStateDrive";
        dev.capacity = capacity_bytes;
        dev.free = capacity_bytes;
        node.ssd_devices.push_back(std::move(dev));
        next[node.node_id] = std::move(node);
    }

    for (uint32_t i = 0; i < hdd_nodes; ++i) {
        NodeState node;
        node.node_id = "node_hdd_" + std::to_string(i);
        node.type = 1;
        DeviceState dev;
        dev.device_id = node.node_id + "_HDD_0";
        dev.type = "HardDiskDrive";
        dev.capacity = capacity_bytes;
        dev.free = capacity_bytes;
        node.hdd_devices.push_back(std::move(dev));
        next[node.node_id] = std::move(node);
    }

    for (uint32_t i = 0; i < mix_nodes; ++i) {
        NodeState node;
        node.node_id = "node_mix_" + std::to_string(i);
        node.type = 2;
        uint64_t ssd_cap = capacity_bytes / 2;
        uint64_t hdd_cap = capacity_bytes - ssd_cap;
        DeviceState ssd;
        ssd.device_id = node.node_id + "_SSD_0";
        ssd.type = "SolidStateDrive";
        ssd.capacity = ssd_cap;
        ssd.free = ssd_cap;
        node.ssd_devices.push_back(std::move(ssd));
        DeviceState hdd;
        hdd.device_id = node.node_id + "_HDD_0";
        hdd.type = "HardDiskDrive";
        hdd.capacity = hdd_cap;
        hdd.free = hdd_cap;
        node.hdd_devices.push_back(std::move(hdd));
        next[node.node_id] = std::move(node);
    }

    {
        std::lock_guard<std::mutex> lk(mu_);
        nodes_ = std::move(next);
        RebuildIndex();
        dirty_ = false;
    }
    return true;
}

std::vector<VirtualNodeLedger::NodeSummary> VirtualNodeLedger::ListNodes() const {
    std::vector<NodeSummary> out;
    std::lock_guard<std::mutex> lk(mu_);
    out.reserve(nodes_.size());
    for (const auto& kv : nodes_) {
        const auto& node = kv.second;
        uint64_t total = 0;
        uint64_t free = 0;
        for (const auto& dev : node.ssd_devices) {
            total += dev.capacity;
            free += dev.free;
        }
        for (const auto& dev : node.hdd_devices) {
            total += dev.capacity;
            free += dev.free;
        }
        out.push_back(NodeSummary{node.node_id, node.type, total, free});
    }
    return out;
}

bool VirtualNodeLedger::GetNodeCapacity(const std::string& node_id, uint64_t* total_bytes, uint64_t* free_bytes) const {
    if (!total_bytes || !free_bytes) {
        return false;
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    uint64_t total = 0;
    uint64_t free = 0;
    for (const auto& dev : it->second.ssd_devices) {
        total += dev.capacity;
        free += dev.free;
    }
    for (const auto& dev : it->second.hdd_devices) {
        total += dev.capacity;
        free += dev.free;
    }
    *total_bytes = total;
    *free_bytes = free;
    return true;
}

bool VirtualNodeLedger::ApplyInode(const Inode& inode, std::string* out_node_id) {
    const uint8_t type = static_cast<uint8_t>(inode.location_id.fields.node_type & 0x03);
    const uint16_t idx = inode.location_id.fields.node_id;
    const uint64_t bytes = inode.getFileSize();

    std::lock_guard<std::mutex> lk(mu_);
    const std::string node_id = ResolveNodeId(idx, type);
    if (out_node_id) {
        *out_node_id = node_id;
    }
    if (node_id.empty()) {
        return false;
    }
    if (bytes == 0) {
        return true;
    }
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }

    auto consume = [](std::vector<DeviceState>& devices, uint64_t& remaining) {
        for (auto& dev : devices) {
            if (remaining == 0) {
                return;
            }
            if (dev.free == 0) {
                continue;
            }
            uint64_t take = dev.free < remaining ? dev.free : remaining;
            dev.free -= take;
            dev.used += take;
            remaining -= take;
        }
    };

    uint64_t remaining = bytes;
    switch (type) {
        case 0:
            consume(it->second.ssd_devices, remaining);
            if (remaining > 0) {
                consume(it->second.hdd_devices, remaining);
            }
            break;
        case 1:
            consume(it->second.hdd_devices, remaining);
            if (remaining > 0) {
                consume(it->second.ssd_devices, remaining);
            }
            break;
        case 2:
        default:
            consume(it->second.ssd_devices, remaining);
            consume(it->second.hdd_devices, remaining);
            break;
    }

    if (remaining != bytes) {
        dirty_ = true;
    }
    return remaining == 0;
}

bool VirtualNodeLedger::TakeDirty() {
    std::lock_guard<std::mutex> lk(mu_);
    bool was = dirty_;
    dirty_ = false;
    return was;
}

std::string VirtualNodeLedger::ResolveNodeId(uint16_t idx, uint8_t type) const {
    const std::vector<std::string>* list = nullptr;
    if (type == 0) {
        list = &ssd_nodes_;
    } else if (type == 1) {
        list = &hdd_nodes_;
    } else {
        list = &mix_nodes_;
    }
    if (list && !list->empty()) {
        if (idx < list->size() && !(*list)[idx].empty()) {
            return (*list)[idx];
        }
        for (const auto& id : *list) {
            if (!id.empty()) {
                return id;
            }
        }
    }
    for (const auto& kv : nodes_) {
        if (!kv.first.empty()) {
            return kv.first;
        }
    }
    return "";
}

bool VirtualNodeLedger::Allocate(NodeState& node, uint64_t bytes) {
    uint64_t remaining = bytes;
    auto consume = [](std::vector<DeviceState>& devices, uint64_t& rem) {
        for (auto& dev : devices) {
            if (rem == 0) {
                return;
            }
            if (dev.free == 0) {
                continue;
            }
            uint64_t take = dev.free < rem ? dev.free : rem;
            dev.free -= take;
            dev.used += take;
            rem -= take;
        }
    };
    consume(node.ssd_devices, remaining);
    consume(node.hdd_devices, remaining);
    return remaining == 0;
}

void VirtualNodeLedger::RebuildIndex() {
    ssd_nodes_.clear();
    hdd_nodes_.clear();
    mix_nodes_.clear();

    for (const auto& kv : nodes_) {
        const auto& node = kv.second;
        std::vector<std::string>* target = nullptr;
        const char* prefix = "";
        if (node.type == 0) {
            target = &ssd_nodes_;
            prefix = "node_ssd_";
        } else if (node.type == 1) {
            target = &hdd_nodes_;
            prefix = "node_hdd_";
        } else {
            target = &mix_nodes_;
            prefix = "node_mix_";
        }
        uint16_t idx = 0;
        if (ParseIndex(node.node_id, prefix, &idx)) {
            if (idx >= target->size()) {
                target->resize(static_cast<size_t>(idx) + 1);
            }
            (*target)[idx] = node.node_id;
        } else {
            target->push_back(node.node_id);
        }
    }
}
