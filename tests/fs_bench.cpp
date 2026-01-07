#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

// Reuse common log redirection helper.
#include "common/LogRedirect.h"

struct Options {
    std::string path;
    std::string log_file;
    int duration_sec = 30;
    int file_size_kb = 64;
    int files = 100;
    int threads = 4;
    int read_pct = 50;
    bool fsync = false;
    int latency_samples = 20000;
};

struct Metrics {
    uint64_t ops = 0;
    uint64_t read_ops = 0;
    uint64_t write_ops = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
    uint64_t errors = 0;
    uint64_t latency_sum_ns = 0;
    uint64_t latency_min_ns = 0;
    uint64_t latency_max_ns = 0;
    uint64_t sample_seen = 0;
    std::vector<uint64_t> latency_samples;
};

static void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "Usage: %s --path=DIR [--duration_sec=N] [--file_size_kb=N]\n"
                 "          [--files=N] [--threads=N] [--read_pct=0-100]\n"
                 "          [--fsync] [--latency_samples=N] [--log_file=PATH]\n",
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
        } else if (match("--duration_sec", &opts->duration_sec)) {
        } else if (match("--file_size_kb", &opts->file_size_kb)) {
        } else if (match("--files", &opts->files)) {
        } else if (match("--threads", &opts->threads)) {
        } else if (match("--read_pct", &opts->read_pct)) {
        } else if (match("--latency_samples", &opts->latency_samples)) {
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
    if (opts->read_pct < 0) opts->read_pct = 0;
    if (opts->read_pct > 100) opts->read_pct = 100;
    if (opts->threads <= 0) opts->threads = 1;
    if (opts->files <= 0) opts->files = 1;
    if (opts->file_size_kb <= 0) opts->file_size_kb = 1;
    if (opts->duration_sec <= 0) opts->duration_sec = 1;
    if (opts->latency_samples < 0) opts->latency_samples = 0;
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

static bool WriteFile(const std::string& path, const std::vector<char>& data, bool do_fsync,
                      uint64_t* out_bytes) {
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) return false;
    size_t left = data.size();
    const char* ptr = data.data();
    uint64_t total = 0;
    while (left > 0) {
        ssize_t n = ::write(fd, ptr, left);
        if (n < 0) {
            ::close(fd);
            return false;
        }
        left -= static_cast<size_t>(n);
        ptr += n;
        total += static_cast<uint64_t>(n);
    }
    if (do_fsync) {
        ::fsync(fd);
    }
    ::close(fd);
    if (out_bytes) *out_bytes = total;
    return true;
}

static bool ReadFile(const std::string& path, std::vector<char>& buf, uint64_t* out_bytes) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    uint64_t total = 0;
    while (true) {
        ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            ::close(fd);
            return false;
        }
        if (n == 0) break;
        total += static_cast<uint64_t>(n);
    }
    ::close(fd);
    if (out_bytes) *out_bytes = total;
    return true;
}

static void RecordLatency(Metrics& m, uint64_t latency_ns, std::mt19937_64& rng,
                          uint64_t sample_cap) {
    m.ops++;
    m.latency_sum_ns += latency_ns;
    if (m.latency_min_ns == 0 || latency_ns < m.latency_min_ns) m.latency_min_ns = latency_ns;
    if (latency_ns > m.latency_max_ns) m.latency_max_ns = latency_ns;
    if (sample_cap == 0) return;
    m.sample_seen++;
    if (m.latency_samples.size() < sample_cap) {
        m.latency_samples.push_back(latency_ns);
        return;
    }
    std::uniform_int_distribution<uint64_t> pick(0, m.sample_seen - 1);
    uint64_t idx = pick(rng);
    if (idx < sample_cap) {
        m.latency_samples[idx] = latency_ns;
    }
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

    const std::string bench_dir = JoinPath(opts.path, "zb_bench");
    if (!EnsureDir(opts.path) || !EnsureDir(bench_dir)) {
        return 2;
    }

    const size_t file_size = static_cast<size_t>(opts.file_size_kb) * 1024;
    std::vector<char> init_buf(file_size, 'z');
    for (int i = 0; i < opts.files; ++i) {
        std::string f = JoinPath(bench_dir, "file_" + std::to_string(i));
        uint64_t written = 0;
        if (!WriteFile(f, init_buf, opts.fsync, &written)) {
            std::cerr << "Init write failed: " << f << " err=" << std::strerror(errno) << std::endl;
            return 3;
        }
    }

    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    std::vector<Metrics> stats(opts.threads);

    const auto start_time = std::chrono::steady_clock::now();
    const auto end_time = start_time + std::chrono::seconds(opts.duration_sec);

    for (int t = 0; t < opts.threads; ++t) {
        workers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t + 1) * 0x9e3779b97f4a7c15ULL);
            std::uniform_int_distribution<int> pick_file(0, opts.files - 1);
            std::uniform_int_distribution<int> pick_op(0, 99);
            std::vector<char> buf(file_size, 'a' + (t % 26));
            uint64_t sample_cap = static_cast<uint64_t>(opts.latency_samples);
            int error_logs = 0;
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (std::chrono::steady_clock::now() < end_time) {
                const bool do_read = pick_op(rng) < opts.read_pct;
                const int idx = pick_file(rng);
                const std::string path = JoinPath(bench_dir, "file_" + std::to_string(idx));
                const auto op_start = std::chrono::steady_clock::now();
                uint64_t bytes = 0;
                bool ok = false;
                if (do_read) {
                    ok = ReadFile(path, buf, &bytes);
                } else {
                    ok = WriteFile(path, buf, opts.fsync, &bytes);
                }
                const auto op_end = std::chrono::steady_clock::now();
                const uint64_t latency_ns =
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              op_end - op_start)
                                              .count());
                RecordLatency(stats[t], latency_ns, rng, sample_cap);
                if (do_read) {
                    stats[t].read_ops++;
                    stats[t].bytes_read += bytes;
                } else {
                    stats[t].write_ops++;
                    stats[t].bytes_written += bytes;
                }
                if (!ok) {
                    stats[t].errors++;
                    if (error_logs < 5) {
                        std::cerr << "[bench] op failed path=" << path
                                  << " err=" << std::strerror(errno) << std::endl;
                        error_logs++;
                    }
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& th : workers) {
        th.join();
    }

    Metrics total;
    std::vector<uint64_t> samples;
    for (const auto& s : stats) {
        total.ops += s.ops;
        total.read_ops += s.read_ops;
        total.write_ops += s.write_ops;
        total.bytes_read += s.bytes_read;
        total.bytes_written += s.bytes_written;
        total.errors += s.errors;
        total.latency_sum_ns += s.latency_sum_ns;
        if (total.latency_min_ns == 0 || (s.latency_min_ns && s.latency_min_ns < total.latency_min_ns)) {
            total.latency_min_ns = s.latency_min_ns;
        }
        if (s.latency_max_ns > total.latency_max_ns) {
            total.latency_max_ns = s.latency_max_ns;
        }
        samples.insert(samples.end(), s.latency_samples.begin(), s.latency_samples.end());
    }

    const double elapsed_sec = static_cast<double>(opts.duration_sec);
    const double qps = elapsed_sec > 0 ? total.ops / elapsed_sec : 0.0;
    const double read_qps = elapsed_sec > 0 ? total.read_ops / elapsed_sec : 0.0;
    const double write_qps = elapsed_sec > 0 ? total.write_ops / elapsed_sec : 0.0;
    const double read_mb = static_cast<double>(total.bytes_read) / (1024.0 * 1024.0);
    const double write_mb = static_cast<double>(total.bytes_written) / (1024.0 * 1024.0);
    const double read_bw = elapsed_sec > 0 ? read_mb / elapsed_sec : 0.0;
    const double write_bw = elapsed_sec > 0 ? write_mb / elapsed_sec : 0.0;

    std::sort(samples.begin(), samples.end());
    auto percentile = [&](double pct) -> double {
        if (samples.empty()) return 0.0;
        double idx = (pct / 100.0) * (samples.size() - 1);
        size_t lo = static_cast<size_t>(idx);
        size_t hi = std::min(lo + 1, samples.size() - 1);
        double frac = idx - lo;
        return samples[lo] * (1.0 - frac) + samples[hi] * frac;
    };

    const double avg_us = total.ops ? (total.latency_sum_ns / 1000.0) / total.ops : 0.0;
    const double p50_us = percentile(50.0) / 1000.0;
    const double p95_us = percentile(95.0) / 1000.0;
    const double p99_us = percentile(99.0) / 1000.0;

    std::printf("fs_bench results\n");
    std::printf("  path: %s\n", opts.path.c_str());
    std::printf("  threads: %d  duration_sec: %d  file_size_kb: %d  files: %d\n",
                opts.threads, opts.duration_sec, opts.file_size_kb, opts.files);
    std::printf("  read_pct: %d  fsync: %s\n", opts.read_pct, opts.fsync ? "true" : "false");
    std::printf("  ops: %llu  read_ops: %llu  write_ops: %llu  errors: %llu\n",
                static_cast<unsigned long long>(total.ops),
                static_cast<unsigned long long>(total.read_ops),
                static_cast<unsigned long long>(total.write_ops),
                static_cast<unsigned long long>(total.errors));
    std::printf("  qps: %.2f  read_qps: %.2f  write_qps: %.2f\n", qps, read_qps, write_qps);
    std::printf("  bw_read_MBps: %.2f  bw_write_MBps: %.2f\n", read_bw, write_bw);
    std::printf("  latency_us: avg=%.2f p50=%.2f p95=%.2f p99=%.2f min=%.2f max=%.2f\n",
                avg_us, p50_us, p95_us, p99_us,
                total.latency_min_ns / 1000.0, total.latency_max_ns / 1000.0);
    return 0;
}
