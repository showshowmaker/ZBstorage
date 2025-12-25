#include "IOEngine.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <system_error>

namespace fs = std::filesystem;

namespace {

std::string MakeKey(const std::string& path, int flags) {
    return path + "#" + std::to_string(flags);
}

} // namespace

IOEngine::IOEngine(std::string base_path, Options opts)
    : base_path_(std::move(base_path)), opts_(opts) {
    if (opts_.max_open_files == 0) {
        opts_.max_open_files = 1;
    }
}

IOEngine::~IOEngine() {
    std::lock_guard<std::mutex> lk(fd_mu_);
    for (auto& kv : fd_cache_) {
        if (kv.second.fd >= 0) {
            ::close(kv.second.fd);
        }
    }
    fd_cache_.clear();
    lru_.clear();
}

int IOEngine::NormalizeFlags(int flags, bool write_access) const {
    int f = flags;
    if (f == 0) {
        f = write_access ? (O_WRONLY | O_CREAT) : O_RDONLY;
    }
    if ((f & (O_WRONLY | O_RDWR)) == 0 && write_access) {
        f |= O_WRONLY;
    }
    if (opts_.sync_on_write && write_access) {
        f |= O_DSYNC;
    }
    f |= O_CLOEXEC;
    return f;
}

int IOEngine::AcquireFd(const std::string& path,
                        int flags,
                        bool create_if_missing,
                        int mode,
                        int& err) {
    bool write_access = (flags & (O_WRONLY | O_RDWR)) != 0;
    int normalized = NormalizeFlags(flags, write_access);
    int cache_flags = normalized & ~O_CREAT;
    const std::string key = MakeKey(path, cache_flags);

    std::lock_guard<std::mutex> lk(fd_mu_);
    auto it = fd_cache_.find(key);
    if (it != fd_cache_.end()) {
        lru_.splice(lru_.begin(), lru_, it->second.lru_it);
        it->second.lru_it = lru_.begin();
        ++it->second.ref_count;
        err = 0;
        return it->second.fd;
    }

    if (create_if_missing) {
        std::error_code ec;
        fs::create_directories(fs::path(path).parent_path(), ec);
        normalized |= O_CREAT;
    } else {
        normalized &= ~O_CREAT;
    }

    int fd = ::open(path.c_str(), normalized, mode);
    if (fd < 0) {
        err = errno;
        return -1;
    }

    lru_.push_front(key);
    FDEntry entry;
    entry.fd = fd;
    entry.writable = write_access;
    entry.lru_it = lru_.begin();
    entry.ref_count = 1;
    fd_cache_.emplace(key, entry);

    EvictIfNeeded();
    err = 0;
    return fd;
}

void IOEngine::ReleaseFd(const std::string& path, int flags) {
    bool write_access = (flags & (O_WRONLY | O_RDWR)) != 0;
    int normalized = NormalizeFlags(flags, write_access);
    int cache_flags = normalized & ~O_CREAT;
    const std::string key = MakeKey(path, cache_flags);

    std::lock_guard<std::mutex> lk(fd_mu_);
    auto it = fd_cache_.find(key);
    if (it == fd_cache_.end()) {
        return;
    }
    if (it->second.ref_count > 0) {
        --it->second.ref_count;
    }
    if (it->second.ref_count == 0) {
        EvictIfNeeded();
    }
}

void IOEngine::EvictIfNeeded() {
    while (fd_cache_.size() > opts_.max_open_files && !lru_.empty()) {
        const std::string& victim = lru_.back();
        auto it = fd_cache_.find(victim);
        if (it != fd_cache_.end()) {
            if (it->second.ref_count > 0) {
                lru_.splice(lru_.begin(), lru_, it->second.lru_it);
                it->second.lru_it = lru_.begin();
                return;
            }
            ::close(it->second.fd);
            fd_cache_.erase(it);
        }
        lru_.pop_back();
    }
}

IOEngine::Result IOEngine::Write(const std::string& path,
                                 const void* data,
                                 size_t size,
                                 uint64_t offset,
                                 int flags,
                                 int mode) {
    Result r{};
    int err = 0;
    int fd = AcquireFd(path, flags, /*create_if_missing=*/true, mode, err);
    if (fd < 0) {
        r.bytes = -1;
        r.err = err;
        return r;
    }

    ssize_t n = ::pwrite(fd, data, size, static_cast<off_t>(offset));
    if (n < 0) {
        r.bytes = -1;
        r.err = errno;
        ReleaseFd(path, flags);
        return r;
    }
    r.bytes = n;
    if (opts_.sync_on_write) {
        if (::fsync(fd) != 0) {
            r.err = errno;
        }
    }
    ReleaseFd(path, flags);
    return r;
}

IOEngine::Result IOEngine::Read(const std::string& path,
                                uint64_t offset,
                                size_t length,
                                std::string& out,
                                int flags) {
    Result r{};
    out.resize(length);

    int err = 0;
    int fd = AcquireFd(path, flags, /*create_if_missing=*/false, 0, err);
    if (fd < 0) {
        r.bytes = -1;
        r.err = err;
        out.clear();
        return r;
    }

    ssize_t n = ::pread(fd, out.data(), length, static_cast<off_t>(offset));
    if (n < 0) {
        r.bytes = -1;
        r.err = errno;
        out.clear();
        ReleaseFd(path, flags);
        return r;
    }
    r.bytes = n;
    out.resize(static_cast<size_t>(n));
    ReleaseFd(path, flags);
    return r;
}
