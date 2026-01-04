#include "DiskManager.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <utility>

#ifdef __linux__
#include <mntent.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#endif

namespace fs = std::filesystem;

DiskManager::DiskManager(DiskMountConfig config)
    : config_(std::move(config)) {
    if (config_.fs_type.empty()) {
        config_.fs_type = "ext4";
    }
}

bool DiskManager::Prepare() {
    if (config_.mount_point.empty()) {
        std::cerr << "DiskManager: mount_point is empty" << std::endl;
        return false;
    }
    if (config_.skip_mount) {
        if (!EnsureMountPoint()) {
            std::cerr << "DiskManager: failed to ensure mount point " << config_.mount_point << std::endl;
            return false;
        }
        return RefreshStats();
    }
    if (!EnsureMountPoint()) {
        std::cerr << "DiskManager: failed to ensure mount point " << config_.mount_point << std::endl;
        return false;
    }

    if (config_.auto_mount) {
        if (!MountIfRequired()) {
            return false;
        }
    } else if (!IsMounted() && !config_.device_path.empty()) {
        std::cerr << "DiskManager: device " << config_.device_path << " not mounted at "
                  << config_.mount_point << std::endl;
        return false;
    }

    return RefreshStats();
}

bool DiskManager::Refresh() {
    return RefreshStats();
}

bool DiskManager::EnsureMountPoint() const {
    std::error_code ec;
    fs::path mount_path(config_.mount_point);
    if (fs::exists(mount_path, ec)) {
        return fs::is_directory(mount_path, ec);
    }
    return fs::create_directories(mount_path, ec);
}

bool DiskManager::IsMounted() const {
#ifdef __linux__
    FILE* fp = setmntent("/proc/mounts", "r");
    if (!fp) {
        std::cerr << "DiskManager: failed to open /proc/mounts" << std::endl;
        return false;
    }
    struct mntent* ent = nullptr;
    while ((ent = getmntent(fp)) != nullptr) {
        if (config_.mount_point == ent->mnt_dir) {
            endmntent(fp);
            return true;
        }
        if (!config_.device_path.empty() && config_.device_path == ent->mnt_fsname) {
            endmntent(fp);
            return true;
        }
    }
    endmntent(fp);
    return false;
#else
    // On non-Linux platforms, treat the existence of the mount path as readiness.
    std::error_code ec;
    return fs::exists(config_.mount_point, ec);
#endif
}

bool DiskManager::MountIfRequired() {
#ifdef __linux__
    if (IsMounted()) {
        return true;
    }
    if (config_.device_path.empty()) {
        std::cerr << "DiskManager: device_path is empty, cannot mount automatically" << std::endl;
        return false;
    }
    if (::mount(config_.device_path.c_str(),
                config_.mount_point.c_str(),
                config_.fs_type.c_str(),
                MS_RELATIME,
                nullptr) != 0) {
        int err = errno;
        if (err == EPERM || err == EACCES) {
            std::cerr << "DiskManager: mount skipped due to insufficient privileges ("
                      << std::strerror(err) << "), expecting external mount" << std::endl;
            return IsMounted();
        }
        std::cerr << "DiskManager: mount failed (" << std::strerror(err) << ")" << std::endl;
        return false;
    }
    return true;
#else
    return false;
#endif
}

bool DiskManager::RefreshStats() {
#ifdef __linux__
    struct statvfs st {};
    if (statvfs(config_.mount_point.c_str(), &st) != 0) {
        std::cerr << "DiskManager: statvfs failed (" << std::strerror(errno) << ")" << std::endl;
        std::error_code ec;
        auto sp = fs::space(config_.mount_point, ec);
        if (ec) {
            std::cerr << "DiskManager: filesystem::space failed (" << ec.message() << ")" << std::endl;
            return false;
        }
        stats_.total_bytes = sp.capacity;
        stats_.free_bytes = sp.available;
        return true;
    }
    stats_.total_bytes = static_cast<uint64_t>(st.f_blocks) * st.f_frsize;
    stats_.free_bytes = static_cast<uint64_t>(st.f_bavail) * st.f_frsize;
    return true;
#else
    std::error_code ec;
    auto sp = fs::space(config_.mount_point, ec);
    if (ec) {
        std::cerr << "DiskManager: filesystem::space failed (" << ec.message() << ")" << std::endl;
        return false;
    }
    stats_.total_bytes = sp.capacity;
    stats_.free_bytes = sp.available;
    return true;
#endif
}
