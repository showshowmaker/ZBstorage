#pragma once

#include <cstdint>
#include <string>

struct DiskMountConfig {
    std::string device_path;
    std::string mount_point;
    std::string fs_type;
    bool auto_mount{true};
};

struct DiskStats {
    uint64_t total_bytes{0};
    uint64_t free_bytes{0};
};

class DiskManager {
public:
    explicit DiskManager(DiskMountConfig config);

    bool Prepare();
    bool IsMounted() const;
    bool Refresh();
    DiskStats Stats() const { return stats_; }
    const std::string& mount_point() const { return config_.mount_point; }
    const std::string& device_path() const { return config_.device_path; }

private:
    bool EnsureMountPoint() const;
    bool MountIfRequired();
    bool RefreshStats();

    DiskMountConfig config_;
    DiskStats stats_{};
};
