#include "IOEngine.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>

namespace fs = std::filesystem;

IOEngine::IOEngine(std::string base_path, Options opts)
    : base_path_(std::move(base_path)), opts_(opts) {
    if (!base_path_.empty() && base_path_.back() == '/') {
        base_path_.pop_back();
    }
}

IOEngine::~IOEngine() {
    // no cached resources to release
}

std::string IOEngine::PathFor(uint64_t chunk_id) const {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << chunk_id;
    const std::string hex = oss.str();
    const std::string d1 = hex.substr(0, 2);
    const std::string d2 = hex.substr(2, 2);
    fs::path p(base_path_);
    p /= d1;
    p /= d2;
    p /= "chunk_" + std::to_string(chunk_id);
    return p.string();
}

IOEngine::Result IOEngine::Write(uint64_t chunk_id,
                                 const void* data,
                                 size_t size,
                                 uint64_t offset) {
    Result r{};
    auto path = PathFor(chunk_id);
    std::error_code ec;
    fs::create_directories(fs::path(path).parent_path(), ec);
    int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) {
        r.bytes = -1;
        r.err = errno;
        return r;
    }
    ssize_t n = ::pwrite(fd, data, size, static_cast<off_t>(offset));
    if (n < 0) {
        r.bytes = -1;
        r.err = errno;
        ::close(fd);
        return r;
    }
    r.bytes = n;
    if (opts_.sync_on_write) {
        if (::fsync(fd) != 0) {
            r.err = errno;
        }
    }
    ::close(fd);
    return r;
}

IOEngine::Result IOEngine::Read(uint64_t chunk_id,
                                uint64_t offset,
                                size_t length,
                                std::string& out) {
    Result r{};
    out.assign(length, '\0');
    auto path = PathFor(chunk_id);
    std::error_code ec;
    fs::create_directories(fs::path(path).parent_path(), ec);

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0 && errno == ENOENT) {
        // 文件不存在则创建空文件再读
        fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
        if (fd < 0) {
            r.bytes = -1;
            r.err = errno;
            out.clear();
            return r;
        }
    }
    if (fd < 0) {
        r.bytes = -1;
        r.err = errno;
        out.clear();
        return r;
    }
    ssize_t n = ::pread(fd, out.data(), length, static_cast<off_t>(offset));
    if (n < 0) {
        r.bytes = -1;
        r.err = errno;
        out.clear();
        ::close(fd);
        return r;
    }
    r.bytes = n;
    out.resize(static_cast<size_t>(n));
    ::close(fd);
    return r;
}
