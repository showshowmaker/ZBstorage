#include "LocalMetadataManager.h"

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace fs = std::filesystem;

namespace {

std::string EnsureTrailingSlash(const std::string& s) {
    if (!s.empty() && s.back() == '/') {
        return s.substr(0, s.size() - 1);
    }
    return s;
}

} // namespace

LocalMetadataManager::LocalMetadataManager(std::vector<std::string> data_roots, std::string manifest_path)
    : data_roots_(std::move(data_roots)) {
    for (auto& root : data_roots_) {
        root = EnsureTrailingSlash(root);
        std::error_code ec;
        fs::create_directories(root, ec);
    }
    if (data_roots_.empty()) {
        std::cerr << "LocalMetadataManager: no data roots configured" << std::endl;
        return;
    }
    if (manifest_path.empty()) {
        manifest_path_ = data_roots_[0] + "/chunk_manifest.log";
    } else {
        manifest_path_ = manifest_path;
    }
    LoadManifest();
    manifest_out_.open(manifest_path_, std::ios::out | std::ios::app);
    if (!manifest_out_.is_open()) {
        std::cerr << "LocalMetadataManager: failed to open manifest " << manifest_path_ << std::endl;
    }
}

LocalMetadataManager::~LocalMetadataManager() {
    if (manifest_out_.is_open()) {
        manifest_out_.flush();
        manifest_out_.close();
    }
}

bool LocalMetadataManager::LoadManifest() {
    std::error_code ec;
    if (!fs::exists(manifest_path_, ec)) {
        return true;
    }
    std::ifstream in(manifest_path_);
    if (!in.is_open()) {
        std::cerr << "LocalMetadataManager: failed to read manifest " << manifest_path_ << std::endl;
        return false;
    }
    std::string op;
    uint64_t chunk_id = 0;
    std::string path;
    while (in >> op >> chunk_id >> path) {
        if (op == "ADD") {
            path_map_[chunk_id] = path;
        } else if (op == "DEL") {
            path_map_.erase(chunk_id);
        }
    }
    return true;
}

std::string LocalMetadataManager::GetPath(uint64_t chunk_id) const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    auto it = path_map_.find(chunk_id);
    if (it != path_map_.end()) {
        return it->second;
    }
    return {};
}

std::string LocalMetadataManager::AllocPath(uint64_t chunk_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto it = path_map_.find(chunk_id);
    if (it != path_map_.end()) {
        return it->second;
    }
    if (data_roots_.empty()) {
        return {};
    }
    const std::string& root = data_roots_[next_root_ % data_roots_.size()];
    ++next_root_;
    std::string rel = ShardedRelativePath(chunk_id);
    std::string full_path = root + "/" + rel;

    std::error_code ec;
    fs::create_directories(fs::path(full_path).parent_path(), ec);

    path_map_[chunk_id] = full_path;
    AppendRecord("ADD", chunk_id, full_path);
    return full_path;
}

void LocalMetadataManager::DeletePath(uint64_t chunk_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto it = path_map_.find(chunk_id);
    if (it == path_map_.end()) {
        return;
    }
    path_map_.erase(it);
    AppendRecord("DEL", chunk_id, "");
}

bool LocalMetadataManager::AppendRecord(const std::string& op, uint64_t chunk_id, const std::string& path) {
    if (!manifest_out_.is_open()) {
        return false;
    }
    manifest_out_ << op << ' ' << chunk_id << ' ' << path << '\n';
    manifest_out_.flush();
    return true;
}

std::string LocalMetadataManager::ShardedRelativePath(uint64_t chunk_id) const {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << chunk_id;
    const std::string hex = oss.str();
    std::string rel;
    rel.reserve(2 + 1 + 2 + 1 + 6 + 20);
    rel.append(hex, 0, 2);
    rel.push_back('/');
    rel.append(hex, 2, 2);
    rel.push_back('/');
    rel.append("chunk_");
    rel.append(std::to_string(chunk_id));
    return rel;
}
