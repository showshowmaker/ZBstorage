#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "srm/StorageNodeManager.h"
#include "virtual/InodeBatchMonitor.h"
#include "virtual/VirtualNodeLedger.h"

class VirtualNodeController {
public:
    VirtualNodeController(std::shared_ptr<StorageNodeManager> manager,
                          SimulationParams sim_params);
    ~VirtualNodeController();

    VirtualNodeController(const VirtualNodeController&) = delete;
    VirtualNodeController& operator=(const VirtualNodeController&) = delete;

    bool LoadNodesFromJson(const std::string& path, std::string* err);
    bool InitEmptyNodes(uint32_t ssd_nodes, uint32_t hdd_nodes, uint32_t mix_nodes, uint64_t capacity_bytes);
    void StartInodeMonitor(const std::string& dir,
                           const std::string& checkpoint_path,
                           std::chrono::milliseconds poll_interval);
    void StartSnapshot(const std::string& path,
                       std::chrono::seconds interval);
    void Stop();

private:
    void SnapshotLoop();
    void UpdateNodes(const std::vector<std::string>& node_ids);

    std::shared_ptr<StorageNodeManager> manager_;
    SimulationParams sim_params_;
    std::shared_ptr<VirtualNodeLedger> ledger_;
    std::unique_ptr<InodeBatchMonitor> monitor_;

    std::atomic<bool> running_{false};
    std::thread snapshot_thread_;
    std::string snapshot_path_;
    std::chrono::seconds snapshot_interval_{10};
};
