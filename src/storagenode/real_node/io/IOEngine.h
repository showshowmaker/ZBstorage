#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>

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

    IOEngine(std::string base_path, Options opts = {});
    ~IOEngine();

    Result Write(uint64_t chunk_id, const void* data, size_t size, uint64_t offset);
    Result Read(uint64_t chunk_id, uint64_t offset, size_t length, std::string& out);

    std::string PathFor(uint64_t chunk_id) const;

private:
    std::string base_path_;
    Options opts_;
};
