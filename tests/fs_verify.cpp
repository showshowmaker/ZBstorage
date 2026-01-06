#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "common/LogRedirect.h"

struct Options {
    std::string path;
    std::string log_file;
    int file_size_kb = 64;
    int files = 10;
    int iterations = 1;
    bool fsync = false;
    uint64_t seed = 1;
};

static void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "Usage: %s --path=DIR [--file_size_kb=N] [--files=N]\n"
                 "          [--iterations=N] [--fsync] [--seed=N] [--log_file=PATH]\n",
                 prog);
}

static bool ParseArgs(int argc, char* argv[], Options* opts) {
    if (!opts) return false;
    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (std::strcmp(arg, "--help") == 0 || std::strcmp(arg, "-h") == 0) {
            return false;
        }
        auto match = [&](const char* key, int* out) -> bool {
            size_t len = std::strlen(key);
            if (std::strncmp(arg, key, len) == 0 && arg[len] == '=') {
                *out = std::atoi(arg + len + 1);
                return true;
            }
            return false;
        };
        if (std::strncmp(arg, "--path=", 7) == 0) {
            opts->path = arg + 7;
        } else if (std::strncmp(arg, "--log_file=", 11) == 0) {
            opts->log_file = arg + 11;
        } else if (std::strncmp(arg, "--seed=", 7) == 0) {
            opts->seed = static_cast<uint64_t>(std::strtoull(arg + 7, nullptr, 10));
        } else if (match("--file_size_kb", &opts->file_size_kb)) {
        } else if (match("--files", &opts->files)) {
        } else if (match("--iterations", &opts->iterations)) {
        } else if (std::strcmp(arg, "--fsync") == 0) {
            opts->fsync = true;
        } else {
            std::fprintf(stderr, "Unknown arg: %s\n", arg);
            return false;
        }
    }
    if (opts->path.empty()) {
        std::fprintf(stderr, "Missing --path\n");
        return false;
    }
    if (opts->file_size_kb <= 0) opts->file_size_kb = 1;
    if (opts->files <= 0) opts->files = 1;
    if (opts->iterations <= 0) opts->iterations = 1;
    return true;
}

static std::string JoinPath(const std::string& base, const std::string& name) {
    if (base.empty()) return name;
    if (base.back() == '/') return base + name;
    return base + "/" + name;
}

static bool EnsureDir(const std::string& dir) {
    if (::mkdir(dir.c_str(), 0755) == 0) return true;
    if (errno == EEXIST) return true;
    std::cerr << "mkdir failed: " << dir << " err=" << std::strerror(errno) << std::endl;
    return false;
}

static void FillPattern(std::vector<char>& buf, uint64_t seed) {
    uint64_t state = seed ? seed : 1;
    for (size_t i = 0; i < buf.size(); ++i) {
        state = state * 6364136223846793005ULL + 1;
        buf[i] = static_cast<char>(state & 0xFF);
    }
}

static bool WriteFile(const std::string& path, const std::vector<char>& data, bool do_fsync) {
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) return false;
    size_t left = data.size();
    const char* ptr = data.data();
    while (left > 0) {
        ssize_t n = ::write(fd, ptr, left);
        if (n < 0) {
            ::close(fd);
            return false;
        }
        left -= static_cast<size_t>(n);
        ptr += n;
    }
    if (do_fsync) {
        ::fsync(fd);
    }
    ::close(fd);
    return true;
}

static bool ReadFile(const std::string& path, std::vector<char>& buf, size_t expected) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    size_t got = 0;
    while (got < expected) {
        ssize_t n = ::read(fd, buf.data() + got, expected - got);
        if (n < 0) {
            ::close(fd);
            return false;
        }
        if (n == 0) break;
        got += static_cast<size_t>(n);
    }
    ::close(fd);
    return got == expected;
}

int main(int argc, char* argv[]) {
    Options opts;
    if (!ParseArgs(argc, argv, &opts)) {
        PrintUsage(argv[0]);
        return 1;
    }
    if (!opts.log_file.empty()) {
        if (!RedirectLogs(opts.log_file)) {
            std::fprintf(stderr, "Failed to open log file: %s\n", opts.log_file.c_str());
            return 2;
        }
    }

    const std::string verify_dir = JoinPath(opts.path, "zb_verify");
    if (!EnsureDir(opts.path) || !EnsureDir(verify_dir)) {
        return 3;
    }

    const size_t file_size = static_cast<size_t>(opts.file_size_kb) * 1024;
    std::vector<char> write_buf(file_size);
    std::vector<char> read_buf(file_size);

    uint64_t total = 0;
    uint64_t failed = 0;
    for (int iter = 0; iter < opts.iterations; ++iter) {
        for (int i = 0; i < opts.files; ++i) {
            ++total;
            uint64_t seed = opts.seed ^ (static_cast<uint64_t>(iter) << 32) ^ static_cast<uint64_t>(i);
            FillPattern(write_buf, seed);
            const std::string path = JoinPath(verify_dir, "file_" + std::to_string(i));
            if (!WriteFile(path, write_buf, opts.fsync)) {
                std::cerr << "[verify] write failed path=" << path
                          << " err=" << std::strerror(errno) << std::endl;
                ++failed;
                continue;
            }
            struct stat st {};
            if (::stat(path.c_str(), &st) != 0) {
                std::cerr << "[verify] stat failed path=" << path
                          << " err=" << std::strerror(errno) << std::endl;
                ++failed;
                continue;
            }
            if (static_cast<size_t>(st.st_size) != file_size) {
                std::cerr << "[verify] size mismatch path=" << path
                          << " expected=" << file_size
                          << " got=" << st.st_size << std::endl;
                ++failed;
                continue;
            }
            if (!ReadFile(path, read_buf, file_size)) {
                std::cerr << "[verify] read failed path=" << path
                          << " err=" << std::strerror(errno) << std::endl;
                ++failed;
                continue;
            }
            if (std::memcmp(write_buf.data(), read_buf.data(), file_size) != 0) {
                std::cerr << "[verify] data mismatch path=" << path << std::endl;
                ++failed;
            }
        }
    }

    std::printf("fs_verify results\n");
    std::printf("  path: %s\n", opts.path.c_str());
    std::printf("  iterations: %d  files: %d  file_size_kb: %d  fsync: %s\n",
                opts.iterations, opts.files, opts.file_size_kb, opts.fsync ? "true" : "false");
    std::printf("  total_ops: %llu  failed: %llu\n",
                static_cast<unsigned long long>(total),
                static_cast<unsigned long long>(failed));
    return failed == 0 ? 0 : 4;
}
