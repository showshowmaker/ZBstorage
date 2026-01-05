#pragma once

#include <cstdint>
#include <sys/types.h>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

class IOEngine {
public:
    struct Result {
        ssize_t bytes{0};
        int err{0};
    };

    struct Options {
        size_t max_open_files{128};
        bool sync_on_write{false};
    };

    IOEngine(std::string base_path, Options opts = Options{});
    ~IOEngine();

    Result Write(const std::string& path, const void* data, size_t size, uint64_t offset, int flags, int mode);
    Result Read(const std::string& path, uint64_t offset, size_t length, std::string& out, int flags);

private:
    int AcquireFd(const std::string& path, int flags, bool create_if_missing, int mode, int& err);
    void ReleaseFd(const std::string& path, int flags);
    void EvictIfNeeded();
    int NormalizeFlags(int flags, bool write_access) const;

    struct FDEntry {
        int fd{-1};
        bool writable{false};
        std::list<std::string>::iterator lru_it;
        size_t ref_count{0};
    };

    std::mutex fd_mu_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, FDEntry> fd_cache_;

    std::string base_path_;
    Options opts_;
};
