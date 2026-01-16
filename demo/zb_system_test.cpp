#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>

#include <fcntl.h>

#include <gflags/gflags.h>

#include "client/mount/DfsClient.h"
#include "client/mount/MountConfig.h"
#include "mds/inode/InodeStorage.h"

namespace fs = std::filesystem;

DEFINE_string(mds_addr, "127.0.0.1:8010", "MDS address host:port");
DEFINE_string(srm_addr, "127.0.0.1:9100", "SRM address host:port");
DEFINE_string(inode_dir, "", "Directory holding inode batch .bin files");
DEFINE_string(start_file, "", "Start inode batch file (name or full path)");
DEFINE_uint64(start_index, 0, "Start inode index within start_file");
DEFINE_uint32(ssd_nodes, 0, "SSD node count");
DEFINE_uint32(hdd_nodes, 0, "HDD node count");
DEFINE_uint32(mix_nodes, 0, "Mix node count");
DEFINE_uint32(ssd_devices_per_node, 1, "SSD devices per SSD/Mix node");
DEFINE_uint32(hdd_devices_per_node, 1, "HDD devices per HDD/Mix node");
DEFINE_uint64(ssd_capacity_bytes, 0, "SSD device capacity bytes");
DEFINE_uint64(hdd_capacity_bytes, 0, "HDD device capacity bytes");
DEFINE_bool(default_sim, true, "Default to simulation for commands");
DEFINE_string(command, "", "Run a single command and exit (e.g. \"backup 1000 sim\")");

namespace {

struct DeviceState {
    std::string device_id;
    uint64_t capacity{0};
    uint64_t used{0};
};

struct NodeState {
    std::string node_id;
    uint8_t type{0};
    std::vector<DeviceState> ssd_devices;
    std::vector<DeviceState> hdd_devices;
};

struct Stats {
    uint64_t inodes{0};
    uint64_t bytes{0};
    uint64_t failed{0};
    uint64_t missing_node{0};
};

std::vector<std::string> SplitArgs(const std::string& line) {
    std::vector<std::string> out;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        out.push_back(token);
    }
    return out;
}

std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit < (sizeof(units) / sizeof(units[0]) - 1)) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream oss;
    if (unit == 0) {
        oss << static_cast<uint64_t>(value) << units[unit];
    } else {
        oss << std::fixed << std::setprecision(2) << value << units[unit];
    }
    return oss.str();
}

bool MatchesStartFile(const fs::path& path, const std::string& start) {
    if (start.empty()) return true;
    if (path.string() == start) return true;
    return path.filename().string() == start;
}

void Consume(std::vector<DeviceState>& devices, uint64_t& remaining) {
    for (auto& dev : devices) {
        if (remaining == 0) return;
        uint64_t free = dev.capacity > dev.used ? dev.capacity - dev.used : 0;
        if (free == 0) continue;
        uint64_t take = free < remaining ? free : remaining;
        dev.used += take;
        remaining -= take;
    }
}

class SystemTester {
public:
    bool Init() {
        InitRealClient();
        InitSimState();
        return true;
    }

    void RunInteractive() {
        std::cout << "ZBSystemTest ready. Type 'help' for commands.\n";
        std::string line;
        while (true) {
            std::cout << "> ";
            if (!std::getline(std::cin, line)) {
                break;
            }
            auto args = SplitArgs(line);
            if (args.empty()) {
                continue;
            }
            if (args[0] == "exit" || args[0] == "quit") {
                break;
            }
            RunCommand(args);
        }
    }

    void RunOneShot(const std::string& cmd) {
        auto args = SplitArgs(cmd);
        if (args.empty()) {
            std::cerr << "Empty command\n";
            return;
        }
        RunCommand(args);
    }

private:
    void InitRealClient() {
        MountConfig cfg;
        cfg.mds_addr = FLAGS_mds_addr;
        cfg.srm_addr = FLAGS_srm_addr;
        cfg.default_node_id = "node-1";
        real_client_ = std::make_shared<DfsClient>(cfg);
        if (!real_client_->Init()) {
            real_ready_ = false;
            std::cerr << "Real client init failed, real mode disabled.\n";
        } else {
            real_ready_ = true;
        }
    }

    void InitSimState() {
        InitSimNodes();
        if (!FLAGS_inode_dir.empty()) {
            ScanInodeDir();
        }
    }

    void InitSimNodes() {
        sim_nodes_.clear();
        sim_nodes_.reserve(FLAGS_ssd_nodes + FLAGS_hdd_nodes + FLAGS_mix_nodes);

        for (uint32_t i = 0; i < FLAGS_ssd_nodes; ++i) {
            NodeState node;
            node.node_id = "node_ssd_" + std::to_string(i);
            node.type = 0;
            for (uint32_t d = 0; d < FLAGS_ssd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
                dev.capacity = FLAGS_ssd_capacity_bytes;
                node.ssd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        for (uint32_t i = 0; i < FLAGS_hdd_nodes; ++i) {
            NodeState node;
            node.node_id = "node_hdd_" + std::to_string(i);
            node.type = 1;
            for (uint32_t d = 0; d < FLAGS_hdd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
                dev.capacity = FLAGS_hdd_capacity_bytes;
                node.hdd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        for (uint32_t i = 0; i < FLAGS_mix_nodes; ++i) {
            NodeState node;
            node.node_id = "node_mix_" + std::to_string(i);
            node.type = 2;
            for (uint32_t d = 0; d < FLAGS_ssd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
                dev.capacity = FLAGS_ssd_capacity_bytes;
                node.ssd_devices.push_back(std::move(dev));
            }
            for (uint32_t d = 0; d < FLAGS_hdd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
                dev.capacity = FLAGS_hdd_capacity_bytes;
                node.hdd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        sim_node_index_.clear();
        sim_node_index_.reserve(sim_nodes_.size());
        for (size_t i = 0; i < sim_nodes_.size(); ++i) {
            sim_node_index_[sim_nodes_[i].node_id] = i;
        }
    }

    void ScanInodeDir() {
        bin_files_.clear();
        if (FLAGS_inode_dir.empty()) {
            return;
        }
        for (const auto& entry : fs::directory_iterator(FLAGS_inode_dir)) {
            if (!entry.is_regular_file()) continue;
            if (entry.path().extension() == ".bin") {
                bin_files_.push_back(entry.path());
            }
        }
        std::sort(bin_files_.begin(), bin_files_.end());
        if (!FLAGS_start_file.empty()) {
            bool found = false;
            for (size_t i = 0; i < bin_files_.size(); ++i) {
                if (MatchesStartFile(bin_files_[i], FLAGS_start_file)) {
                    current_bin_file_idx_ = i;
                    current_file_offset_ = FLAGS_start_index;
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cerr << "Start file not found: " << FLAGS_start_file << "\n";
            }
        }
    }

    bool ApplyInodeSim(const Inode& inode) {
        const uint8_t type = static_cast<uint8_t>(inode.location_id.fields.node_type & 0x03);
        const uint16_t node_id = inode.location_id.fields.node_id;
        std::string node_name;
        if (type == 0) {
            node_name = "node_ssd_" + std::to_string(node_id);
        } else if (type == 1) {
            node_name = "node_hdd_" + std::to_string(node_id);
        } else {
            node_name = "node_mix_" + std::to_string(node_id);
        }

        auto it = sim_node_index_.find(node_name);
        if (it == sim_node_index_.end()) {
            ++sim_stats_.missing_node;
            return false;
        }
        auto& node = sim_nodes_[it->second];
        uint64_t remaining = inode.getFileSize();
        if (node.type == 0) {
            Consume(node.ssd_devices, remaining);
        } else if (node.type == 1) {
            Consume(node.hdd_devices, remaining);
        } else {
            Consume(node.ssd_devices, remaining);
            if (remaining > 0) {
                Consume(node.hdd_devices, remaining);
            }
        }
        if (remaining > 0) {
            ++sim_stats_.failed;
            return false;
        }
        return true;
    }

    bool SimBackup(uint64_t count) {
        if (bin_files_.empty()) {
            std::cerr << "No inode batch files loaded (check --inode_dir).\n";
            return false;
        }

        const uint64_t slot_size = InodeStorage::INODE_DISK_SLOT_SIZE;
        uint64_t processed = 0;
        auto start = std::chrono::steady_clock::now();

        while (processed < count && current_bin_file_idx_ < bin_files_.size()) {
            const auto& path = bin_files_[current_bin_file_idx_];
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) {
                std::cerr << "Failed to open inode file: " << path.string() << "\n";
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }
            in.seekg(0, std::ios::end);
            const std::streamoff total_bytes = in.tellg();
            if (total_bytes <= 0) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }
            const uint64_t total_slots = static_cast<uint64_t>(total_bytes) / slot_size;
            if (current_file_offset_ >= total_slots) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }
            in.seekg(static_cast<std::streamoff>(current_file_offset_ * slot_size), std::ios::beg);

            std::vector<uint8_t> slot(static_cast<size_t>(slot_size));
            while (processed < count && current_file_offset_ < total_slots) {
                in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot_size));
                if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
                    break;
                }
                size_t off = 0;
                Inode inode;
                if (!Inode::deserialize(slot.data(), off, inode, slot_size)) {
                    ++sim_stats_.failed;
                    ++processed;
                    ++current_file_offset_;
                    continue;
                }
                ++sim_stats_.inodes;
                const uint64_t bytes = inode.getFileSize();
                sim_stats_.bytes += bytes;
                ApplyInodeSim(inode);
                ++processed;
                ++current_file_offset_;
            }
            if (current_file_offset_ >= total_slots) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
            }
        }

        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cout << "Backup done. Inodes=" << processed
                  << ", elapsed=" << elapsed / 1000.0 << "s\n";
        std::cout << "Total files=" << sim_stats_.inodes
                  << ", total bytes=" << FormatBytes(sim_stats_.bytes) << "\n";
        return true;
    }

    void SimCountFileNum() {
        if (sim_stats_.inodes == 0) {
            std::cout << "No simulated files yet.\n";
            return;
        }
        double avg = static_cast<double>(sim_stats_.bytes) / static_cast<double>(sim_stats_.inodes);
        uint64_t used_nodes = 0;
        for (const auto& node : sim_nodes_) {
            uint64_t used = 0;
            for (const auto& dev : node.ssd_devices) used += dev.used;
            for (const auto& dev : node.hdd_devices) used += dev.used;
            if (used > 0) ++used_nodes;
        }
        std::cout << "File count: " << sim_stats_.inodes << "\n";
        std::cout << "Average size: " << FormatBytes(static_cast<uint64_t>(avg)) << "\n";
        std::cout << "Nodes used: " << used_nodes << "\n";
    }

    void SimQueryFile(const std::string& path) {
        size_t h = std::hash<std::string>{}(path);
        uint64_t size = static_cast<uint64_t>(h % (100ULL * 1024 * 1024));
        if (size < 4096) size += 4096;
        double delay_ms = (static_cast<double>(size) / (100.0 * 1024 * 1024)) * 10.0;
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay_ms)));
        std::cout << "Simulated query ok: " << path << "\n";
        std::cout << "Size=" << FormatBytes(size) << ", delay=" << delay_ms << "ms\n";
    }

    void SimWriteFile(const std::string& source_path) {
        std::error_code ec;
        uint64_t size = fs::file_size(source_path, ec);
        if (ec) {
            std::cerr << "Failed to stat file: " << ec.message() << "\n";
            return;
        }
        double seconds = static_cast<double>(size) / (50.0 * 1024 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(seconds * 1000)));
        std::string node_id = sim_nodes_.empty() ? "none" : sim_nodes_[size % sim_nodes_.size()].node_id;
        std::cout << "Simulated write ok. Size=" << FormatBytes(size)
                  << ", node=" << node_id << "\n";
    }

    void RealQueryFile(const std::string& path) {
        if (!real_ready_) {
            std::cerr << "Real client not ready.\n";
            return;
        }
        struct stat st;
        int rc = real_client_->GetAttr(path, &st);
        if (rc != 0) {
            std::cerr << "GetAttr failed: " << rc << "\n";
            return;
        }
        std::cout << "Query ok: " << path << "\n";
        std::cout << "Size=" << st.st_size << " bytes, mode=" << st.st_mode << "\n";
    }

    void RealWriteFile(const std::string& source_path, const std::string& dest_path) {
        if (!real_ready_) {
            std::cerr << "Real client not ready.\n";
            return;
        }
        std::ifstream in(source_path, std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Failed to open source file: " << source_path << "\n";
            return;
        }
        int fd = -1;
        int rc = real_client_->Create(dest_path, O_CREAT | O_WRONLY | O_TRUNC, 0644, fd);
        if (rc != 0) {
            std::cerr << "Create failed: " << rc << "\n";
            return;
        }
        const size_t kBuf = 1 << 20;
        std::vector<char> buf(kBuf);
        size_t total = 0;
        off_t offset = 0;
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            std::streamsize got = in.gcount();
            if (got <= 0) break;
            ssize_t written = 0;
            rc = real_client_->Write(fd, buf.data(), static_cast<size_t>(got), offset, written);
            if (rc != 0) {
                std::cerr << "Write failed: " << rc << "\n";
                break;
            }
            offset += written;
            total += static_cast<size_t>(written);
        }
        real_client_->Close(fd);
        std::cout << "Write ok: " << dest_path << " bytes=" << total << "\n";
    }

    void PrintHelp() {
        std::cout << "Commands:\n"
                  << "  backup <count> [sim|real]\n"
                  << "  count [sim|real]\n"
                  << "  query <path> [sim|real]\n"
                  << "  write <src> [dest] [sim|real]\n"
                  << "  count_total_capacity\n"
                  << "  count_storage_nodes\n"
                  << "  help\n"
                  << "  exit\n";
    }

    void CountTotalCapacity() {
        uint64_t total = 0;
        for (const auto& node : sim_nodes_) {
            for (const auto& dev : node.ssd_devices) total += dev.capacity;
            for (const auto& dev : node.hdd_devices) total += dev.capacity;
        }
        std::cout << "Total simulated capacity: " << FormatBytes(total) << "\n";
    }

    void CountStorageNodes() {
        std::cout << "Total storage nodes: " << sim_nodes_.size() << "\n";
    }

    bool ParseSimFlag(const std::vector<std::string>& args, bool& sim) const {
        if (args.empty()) {
            sim = FLAGS_default_sim;
            return true;
        }
        const std::string& tail = args.back();
        if (tail == "sim") {
            sim = true;
            return true;
        }
        if (tail == "real") {
            sim = false;
            return true;
        }
        sim = FLAGS_default_sim;
        return true;
    }

    void RunCommand(const std::vector<std::string>& args) {
        if (args.empty()) return;
        if (args[0] == "help") {
            PrintHelp();
            return;
        }

        if (args[0] == "backup") {
            if (args.size() < 2) {
                std::cerr << "backup requires count\n";
                return;
            }
            bool sim = FLAGS_default_sim;
            ParseSimFlag(args, sim);
            if (!sim) {
                std::cerr << "Real backup not supported.\n";
                return;
            }
            uint64_t count = std::stoull(args[1]);
            SimBackup(count);
            return;
        }

        if (args[0] == "count") {
            bool sim = FLAGS_default_sim;
            ParseSimFlag(args, sim);
            if (!sim) {
                std::cerr << "Real count not supported.\n";
                return;
            }
            SimCountFileNum();
            return;
        }

        if (args[0] == "query") {
            if (args.size() < 2) {
                std::cerr << "query requires path\n";
                return;
            }
            bool sim = FLAGS_default_sim;
            ParseSimFlag(args, sim);
            if (sim) {
                SimQueryFile(args[1]);
            } else {
                RealQueryFile(args[1]);
            }
            return;
        }

        if (args[0] == "write") {
            if (args.size() < 2) {
                std::cerr << "write requires source file\n";
                return;
            }
            bool sim = FLAGS_default_sim;
            ParseSimFlag(args, sim);
            const std::string& src = args[1];
            std::string dest;
            if (args.size() >= 3 && args[2] != "sim" && args[2] != "real") {
                dest = args[2];
            } else {
                dest = "/" + fs::path(src).filename().string();
            }
            if (sim) {
                SimWriteFile(src);
            } else {
                RealWriteFile(src, dest);
            }
            return;
        }

        if (args[0] == "count_total_capacity") {
            CountTotalCapacity();
            return;
        }

        if (args[0] == "count_storage_nodes") {
            CountStorageNodes();
            return;
        }

        std::cerr << "Unknown command: " << args[0] << "\n";
    }

    std::shared_ptr<DfsClient> real_client_;
    bool real_ready_{false};

    std::vector<NodeState> sim_nodes_;
    std::unordered_map<std::string, size_t> sim_node_index_;
    Stats sim_stats_;
    std::vector<fs::path> bin_files_;
    size_t current_bin_file_idx_{0};
    uint64_t current_file_offset_{0};
};

} // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    SystemTester tester;
    if (!tester.Init()) {
        return 1;
    }

    if (!FLAGS_command.empty()) {
        tester.RunOneShot(FLAGS_command);
        return 0;
    }

    tester.RunInteractive();
    return 0;
}
