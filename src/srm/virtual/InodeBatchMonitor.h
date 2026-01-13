#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "virtual/VirtualNodeLedger.h"

class InodeBatchMonitor {
public:
    using UpdateCallback = std::function<void(const std::vector<std::string>& node_ids)>;

    InodeBatchMonitor(std::string dir,
                      std::string checkpoint_path,
                      std::chrono::milliseconds poll_interval,
                      std::shared_ptr<VirtualNodeLedger> ledger,
                      UpdateCallback on_update);
    ~InodeBatchMonitor();

    InodeBatchMonitor(const InodeBatchMonitor&) = delete;
    InodeBatchMonitor& operator=(const InodeBatchMonitor&) = delete;

    void Start();
    void Stop();

private:
    void Loop();
    void ScanOnce(std::vector<std::string>* updated);
    bool ProcessFile(const std::string& filename, std::unordered_set<std::string>* touched);
    void LoadCheckpoint();
    void SaveCheckpoint();

    std::string dir_;
    std::string checkpoint_path_;
    std::chrono::milliseconds poll_interval_;
    std::shared_ptr<VirtualNodeLedger> ledger_;
    UpdateCallback on_update_;
    std::atomic<bool> running_{false};
    std::thread thread_;

    std::unordered_map<std::string, uint64_t> offsets_;
    bool checkpoint_dirty_{false};
};
