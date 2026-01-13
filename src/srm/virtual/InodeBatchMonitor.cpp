#include "virtual/InodeBatchMonitor.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <unordered_set>

#include <nlohmann/json.hpp>

#include "mds/inode/InodeStorage.h"

namespace fs = std::filesystem;

InodeBatchMonitor::InodeBatchMonitor(std::string dir,
                                     std::string checkpoint_path,
                                     std::chrono::milliseconds poll_interval,
                                     std::shared_ptr<VirtualNodeLedger> ledger,
                                     UpdateCallback on_update)
    : dir_(std::move(dir)),
      checkpoint_path_(std::move(checkpoint_path)),
      poll_interval_(poll_interval),
      ledger_(std::move(ledger)),
      on_update_(std::move(on_update)) {}

InodeBatchMonitor::~InodeBatchMonitor() {
    Stop();
}

void InodeBatchMonitor::Start() {
    if (running_.exchange(true)) {
        return;
    }
    LoadCheckpoint();
    thread_ = std::thread([this]() { Loop(); });
}

void InodeBatchMonitor::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

void InodeBatchMonitor::Loop() {
    while (running_) {
        std::vector<std::string> updated;
        ScanOnce(&updated);
        if (!updated.empty() && on_update_) {
            on_update_(updated);
        }
        if (checkpoint_dirty_) {
            SaveCheckpoint();
        }
        std::this_thread::sleep_for(poll_interval_);
    }
}

void InodeBatchMonitor::ScanOnce(std::vector<std::string>* updated) {
    if (!updated || dir_.empty() || !ledger_) {
        return;
    }
    std::vector<std::string> files;
    try {
        for (const auto& entry : fs::directory_iterator(dir_)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto filename = entry.path().filename().string();
            if (entry.path().extension() == ".bin") {
                files.push_back(filename);
            }
        }
    } catch (...) {
        return;
    }
    if (files.empty()) {
        return;
    }
    std::sort(files.begin(), files.end());
    std::unordered_set<std::string> touched;
    for (const auto& filename : files) {
        ProcessFile(filename, &touched);
    }
    if (!touched.empty()) {
        updated->assign(touched.begin(), touched.end());
        std::sort(updated->begin(), updated->end());
    }
}

bool InodeBatchMonitor::ProcessFile(const std::string& filename, std::unordered_set<std::string>* touched) {
    if (!touched) {
        return false;
    }
    const fs::path path = fs::path(dir_) / filename;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return false;
    }
    in.seekg(0, std::ios::end);
    const std::streamoff end_pos = in.tellg();
    if (end_pos <= 0) {
        return false;
    }

    const uint64_t slot_size = InodeStorage::INODE_DISK_SLOT_SIZE;
    uint64_t file_size = static_cast<uint64_t>(end_pos);
    file_size -= (file_size % slot_size);

    uint64_t offset = 0;
    auto it = offsets_.find(filename);
    if (it != offsets_.end()) {
        offset = it->second;
    }
    if (offset > file_size) {
        offset = 0;
    }
    offset -= (offset % slot_size);
    if (offset >= file_size) {
        return false;
    }

    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    std::vector<uint8_t> slot(static_cast<size_t>(slot_size));
    bool any_touched = false;
    while (offset + slot_size <= file_size) {
        in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot_size));
        if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
            break;
        }
        size_t off = 0;
        Inode inode;
        if (Inode::deserialize(slot.data(), off, inode, slot_size)) {
            std::string node_id;
            if (ledger_->ApplyInode(inode, &node_id) && !node_id.empty()) {
                touched->insert(node_id);
                any_touched = true;
            }
        }
        offset += slot_size;
    }

    offsets_[filename] = offset;
    checkpoint_dirty_ = true;
    return any_touched;
}

void InodeBatchMonitor::LoadCheckpoint() {
    if (checkpoint_path_.empty()) {
        return;
    }
    std::ifstream in(checkpoint_path_);
    if (!in.is_open()) {
        return;
    }
    nlohmann::json root;
    try {
        in >> root;
    } catch (...) {
        return;
    }
    if (!root.contains("files") || !root["files"].is_object()) {
        return;
    }
    offsets_.clear();
    for (auto it = root["files"].begin(); it != root["files"].end(); ++it) {
        offsets_[it.key()] = it.value().get<uint64_t>();
    }
}

void InodeBatchMonitor::SaveCheckpoint() {
    if (checkpoint_path_.empty()) {
        return;
    }
    nlohmann::json root;
    root["files"] = nlohmann::json::object();
    for (const auto& kv : offsets_) {
        root["files"][kv.first] = kv.second;
    }
    fs::path out_path(checkpoint_path_);
    if (!out_path.parent_path().empty()) {
        std::error_code ec;
        fs::create_directories(out_path.parent_path(), ec);
    }
    std::ofstream out(checkpoint_path_, std::ios::trunc);
    if (!out.is_open()) {
        return;
    }
    out << root.dump(2);
    checkpoint_dirty_ = false;
}
