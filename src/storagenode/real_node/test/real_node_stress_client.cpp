#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <fcntl.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>

#include "storage_node.pb.h"

DEFINE_string(server, "127.0.0.1:9010", "Storage real node server address");
DEFINE_int32(ops, 1000, "Total operations to issue");
DEFINE_int32(write_ratio, 30, "Percentage of operations that are writes (0-100)");
DEFINE_int32(data_size, 4096, "Payload size for write/read (bytes)");
DEFINE_uint64(max_chunk_id, 1024, "Max chunk id (inclusive) used for requests");
DEFINE_uint64(max_offset_blocks, 0, "Max offset in payload-sized blocks (0 means offset always 0)");
DEFINE_int32(flags, 0, "POSIX open flags to pass through to server");
DEFINE_int32(mode, 0644, "POSIX mode to use when creating files");
DEFINE_bool(verify_read, false, "If true, verify read data matches last written value for the chunk/offset");

struct Stats {
    int writes{0};
    int reads{0};
    int write_failures{0};
    int read_failures{0};
    int verify_failures{0};
    int64_t total_latency_us{0};
    int completed{0};
};

static std::string MakeKey(uint64_t chunk_id, uint64_t offset) {
    return std::to_string(chunk_id) + "#" + std::to_string(offset);
}

static std::string MakePayload(std::mt19937_64& rng, size_t size) {
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>(byte_dist(rng));
    }
    return data;
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    const int write_ratio = std::clamp(FLAGS_write_ratio, 0, 100);
    const size_t payload_size = static_cast<size_t>(std::max(1, FLAGS_data_size));
    const uint64_t max_chunk = std::max<uint64_t>(1, FLAGS_max_chunk_id);

    brpc::Channel channel;
    brpc::ChannelOptions opts;
    if (channel.Init(FLAGS_server.c_str(), &opts) != 0) {
        std::cerr << "Failed to init channel to " << FLAGS_server << std::endl;
        return -1;
    }
    storagenode::StorageService_Stub stub(&channel);

    std::mt19937_64 rng(123456789);
    std::uniform_int_distribution<int> pct_dist(1, 100);
    std::uniform_int_distribution<uint64_t> chunk_dist(1, max_chunk);
    std::uniform_int_distribution<uint64_t> offset_block_dist(0, FLAGS_max_offset_blocks);

    Stats stats;
    std::unordered_map<std::string, std::string> last_written;

    for (int i = 0; i < FLAGS_ops; ++i) {
        const bool do_write = pct_dist(rng) <= write_ratio;
        const uint64_t chunk_id = chunk_dist(rng);
        const uint64_t offset_blocks =
            FLAGS_max_offset_blocks > 0 ? offset_block_dist(rng) : 0;
        const uint64_t offset = offset_blocks * payload_size;

        if (do_write) {
            std::string payload = MakePayload(rng, payload_size);
            storagenode::WriteRequest req;
            storagenode::WriteReply resp;
            brpc::Controller cntl;
            req.set_chunk_id(chunk_id);
            req.set_offset(offset);
            req.set_data(payload);
            req.set_checksum(0);
            req.set_flags(FLAGS_flags);
            req.set_mode(FLAGS_mode);

            stub.Write(&cntl, &req, &resp, nullptr);
            ++stats.writes;
            if (cntl.Failed() || resp.status().code() != 0) {
                ++stats.write_failures;
                std::cerr << "[WRITE] chunk=" << chunk_id << " offset=" << offset
                          << " failed: " << (cntl.Failed() ? cntl.ErrorText() : resp.status().message())
                          << std::endl;
            } else {
                if (FLAGS_verify_read) {
                    last_written[MakeKey(chunk_id, offset)] = payload;
                }
                stats.total_latency_us += cntl.latency_us();
                ++stats.completed;
            }
        } else {
            storagenode::ReadRequest req;
            storagenode::ReadReply resp;
            brpc::Controller cntl;
            req.set_chunk_id(chunk_id);
            req.set_offset(offset);
            req.set_length(static_cast<uint64_t>(payload_size));
            req.set_flags(FLAGS_flags == 0 ? O_RDONLY : FLAGS_flags);

            stub.Read(&cntl, &req, &resp, nullptr);
            ++stats.reads;
            if (cntl.Failed() || resp.status().code() != 0) {
                ++stats.read_failures;
                std::cerr << "[READ] chunk=" << chunk_id << " offset=" << offset
                          << " failed: " << (cntl.Failed() ? cntl.ErrorText() : resp.status().message())
                          << std::endl;
            } else {
                if (FLAGS_verify_read) {
                    const auto it = last_written.find(MakeKey(chunk_id, offset));
                    if (it != last_written.end() && resp.data() != it->second) {
                        ++stats.verify_failures;
                        std::cerr << "[VERIFY] chunk=" << chunk_id << " offset=" << offset
                                  << " mismatch: expected " << it->second.size()
                                  << " bytes, got " << resp.data().size() << std::endl;
                    }
                }
                stats.total_latency_us += cntl.latency_us();
                ++stats.completed;
            }
        }
    }

    const double avg_latency_ms =
        stats.completed > 0 ? (static_cast<double>(stats.total_latency_us) / 1000.0) / stats.completed : 0.0;

    std::cout << "===== Stress Summary =====" << std::endl;
    std::cout << "server: " << FLAGS_server << std::endl;
    std::cout << "ops (planned): " << FLAGS_ops << " | ops (completed): " << stats.completed << std::endl;
    std::cout << "writes: " << stats.writes << " (failures=" << stats.write_failures << ")" << std::endl;
    std::cout << "reads: " << stats.reads << " (failures=" << stats.read_failures << ")" << std::endl;
    if (FLAGS_verify_read) {
        std::cout << "verify mismatches: " << stats.verify_failures << std::endl;
    }
    std::cout << "avg latency (ms): " << avg_latency_ms << std::endl;

    return (stats.write_failures + stats.read_failures + stats.verify_failures) == 0 ? 0 : -1;
}
