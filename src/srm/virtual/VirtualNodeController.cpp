#include "virtual/VirtualNodeController.h"

#include <iostream>

VirtualNodeController::VirtualNodeController(std::shared_ptr<StorageNodeManager> manager,
                                             SimulationParams sim_params)
    : manager_(std::move(manager)),
      sim_params_(sim_params),
      ledger_(std::make_shared<VirtualNodeLedger>()) {}

VirtualNodeController::~VirtualNodeController() {
    Stop();
}

bool VirtualNodeController::LoadNodesFromJson(const std::string& path, std::string* err) {
    if (!ledger_->LoadFromJson(path, err)) {
        return false;
    }
    if (!manager_) {
        return false;
    }
    auto nodes = ledger_->ListNodes();
    for (const auto& node : nodes) {
        manager_->AddVirtualNode(node.node_id, sim_params_, node.total_bytes);
        manager_->UpdateVirtualNodeCapacity(node.node_id, node.total_bytes, node.free_bytes);
    }
    return true;
}

bool VirtualNodeController::InitEmptyNodes(uint32_t ssd_nodes, uint32_t hdd_nodes, uint32_t mix_nodes, uint64_t capacity_bytes) {
    if (!ledger_->InitEmpty(ssd_nodes, hdd_nodes, mix_nodes, capacity_bytes)) {
        return false;
    }
    if (!manager_) {
        return false;
    }
    auto nodes = ledger_->ListNodes();
    for (const auto& node : nodes) {
        manager_->AddVirtualNode(node.node_id, sim_params_, node.total_bytes);
        manager_->UpdateVirtualNodeCapacity(node.node_id, node.total_bytes, node.free_bytes);
    }
    return true;
}

void VirtualNodeController::StartInodeMonitor(const std::string& dir,
                                              const std::string& checkpoint_path,
                                              std::chrono::milliseconds poll_interval) {
    if (dir.empty()) {
        return;
    }
    monitor_ = std::make_unique<InodeBatchMonitor>(
        dir, checkpoint_path, poll_interval, ledger_,
        [this](const std::vector<std::string>& node_ids) { UpdateNodes(node_ids); });
    monitor_->Start();
}

void VirtualNodeController::StartSnapshot(const std::string& path,
                                          std::chrono::seconds interval) {
    snapshot_path_ = path;
    snapshot_interval_ = interval;
    if (snapshot_path_.empty()) {
        return;
    }
    ledger_->SnapshotToJson(snapshot_path_);
    if (running_.exchange(true)) {
        return;
    }
    snapshot_thread_ = std::thread([this]() { SnapshotLoop(); });
}

void VirtualNodeController::Stop() {
    if (monitor_) {
        monitor_->Stop();
    }
    if (!running_.exchange(false)) {
        return;
    }
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
}

void VirtualNodeController::SnapshotLoop() {
    while (running_) {
        if (ledger_->TakeDirty()) {
            if (!ledger_->SnapshotToJson(snapshot_path_)) {
                std::cerr << "[SRM] failed to write capacity snapshot to " << snapshot_path_ << std::endl;
            }
        }
        std::this_thread::sleep_for(snapshot_interval_);
    }
}

void VirtualNodeController::UpdateNodes(const std::vector<std::string>& node_ids) {
    if (!manager_) {
        return;
    }
    for (const auto& node_id : node_ids) {
        uint64_t total = 0;
        uint64_t free = 0;
        if (!ledger_->GetNodeCapacity(node_id, &total, &free)) {
            continue;
        }
        manager_->UpdateVirtualNodeCapacity(node_id, total, free);
    }
}
