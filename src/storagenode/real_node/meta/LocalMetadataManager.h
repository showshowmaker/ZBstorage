#pragma once

#include <cstdint>
#include <fstream>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// Manages chunk_id -> local path mapping with a simple manifest log for persistence.
class LocalMetadataManager {
public:
    LocalMetadataManager(std::vector<std::string> data_roots, std::string manifest_path = "");
    ~LocalMetadataManager();

    // Returns the full path if present, otherwise empty.
    std::string GetPath(uint64_t chunk_id) const;
    // Allocates a new path for the chunk and persists the mapping; returns empty on failure.
    std::string AllocPath(uint64_t chunk_id);
    // Removes mapping (best-effort) and records a delete marker.
    void DeletePath(uint64_t chunk_id);

private:
    bool LoadManifest();
    bool AppendRecord(const std::string& op, uint64_t chunk_id, const std::string& path);
    std::string ShardedRelativePath(uint64_t chunk_id) const;

    std::vector<std::string> data_roots_;
    std::string manifest_path_;
    mutable std::shared_mutex mu_;
    std::unordered_map<uint64_t, std::string> path_map_;
    size_t next_root_{0};
    std::ofstream manifest_out_;
};
