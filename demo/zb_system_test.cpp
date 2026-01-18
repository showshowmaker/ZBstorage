#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gflags/gflags.h>

#include "mds/inode/InodeStorage.h"

namespace fs = std::filesystem;

DEFINE_string(mds_addr, "127.0.0.1:8010", "MDS address host:port (unused in posix mode)");
DEFINE_string(srm_addr, "127.0.0.1:9100", "SRM address host:port (unused in posix mode)");
DEFINE_string(mount_point, "/mnt/zbstorage", "FUSE mount point for POSIX operations");
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

namespace {

constexpr const char* kMenuSeparator = "========================================";
constexpr const char* kOutputSeparator = "----------------------------------------";

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

void PrintOutputHeader() {
    std::cout << kOutputSeparator << "\n";
}

std::string PromptLine(const std::string& tip) {
    std::cout << tip;
    std::cout.flush();
    std::string line;
    std::getline(std::cin, line);
    return line;
}

uint64_t PromptUint64(const std::string& tip, uint64_t default_value = 0) {
    while (true) {
        std::string line = PromptLine(tip);
        if (line.empty()) {
            return default_value;
        }
        try {
            return std::stoull(line);
        } catch (...) {
            PrintOutputHeader();
            std::cout << u8"???????????\n";
        }
    }
}

std::string MakeMountedPath(const std::string& input) {
    if (input.empty()) {
        return FLAGS_mount_point;
    }
    if (!FLAGS_mount_point.empty() && FLAGS_mount_point.back() == '/') {
        if (!input.empty() && input.front() == '/') {
            return FLAGS_mount_point + input.substr(1);
        }
        return FLAGS_mount_point + input;
    }
    if (!input.empty() && input.front() == '/') {
        return FLAGS_mount_point + input;
    }
    return FLAGS_mount_point + "/" + input;
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

bool CanDeserializeInode(const uint8_t* data, size_t total_size) {
    size_t offset = 0;
    auto safe_read = [&](void* dest, size_t len) -> bool {
        if (offset + len > total_size) return false;
        std::memcpy(dest, data + offset, len);
        offset += len;
        return true;
    };

    Inode inode;
    if (!safe_read(&inode.location_id.raw, sizeof(inode.location_id.raw))) return false;
    if (!safe_read(&inode.block_id, sizeof(inode.block_id))) return false;
    if (!safe_read(&inode.filename_len, sizeof(inode.filename_len))) return false;
    if (!safe_read(&inode.digest_len, sizeof(inode.digest_len))) return false;
    if (!safe_read(&inode.file_mode.raw, sizeof(inode.file_mode.raw))) return false;
    if (!safe_read(&inode.file_size.raw, sizeof(inode.file_size.raw))) return false;
    if (!safe_read(&inode.inode, sizeof(inode.inode))) return false;
    if (offset + Inode::kNamespaceIdLen > total_size) return false;
    offset += Inode::kNamespaceIdLen;
    if (!safe_read(&inode.fm_time, sizeof(inode.fm_time))) return false;
    if (!safe_read(&inode.fa_time, sizeof(inode.fa_time))) return false;
    if (!safe_read(&inode.im_time, sizeof(inode.im_time))) return false;
    if (!safe_read(&inode.fc_time, sizeof(inode.fc_time))) return false;

    if (offset + inode.filename_len + inode.digest_len > total_size) return false;
    offset += inode.filename_len + inode.digest_len;

    uint8_t volume_id_len = 0;
    if (!safe_read(&volume_id_len, sizeof(volume_id_len))) return false;
    if (offset + volume_id_len > total_size) return false;
    offset += volume_id_len;

    uint32_t segment_count = 0;
    if (!safe_read(&segment_count, sizeof(segment_count))) return false;
    if (offset + static_cast<size_t>(segment_count) * sizeof(BlockSegment) > total_size) return false;
    return true;
}

uint64_t DetectSlotSize(std::ifstream& in, uint64_t file_size) {
    std::vector<uint64_t> candidates = {
        InodeStorage::INODE_DISK_SLOT_SIZE,
        1024,
        2048,
        4096
    };
    candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
    uint64_t max_candidate = 0;
    for (auto v : candidates) {
        if (v > max_candidate) max_candidate = v;
    }
    uint64_t read_size = max_candidate;
    if (file_size > 0 && file_size < read_size) {
        read_size = file_size;
    }
    if (read_size == 0) {
        return 0;
    }
    std::vector<uint8_t> buf(static_cast<size_t>(read_size));
    in.clear();
    in.seekg(0, std::ios::beg);
    in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    std::streamsize got = in.gcount();
    if (got <= 0) {
        return 0;
    }
    for (auto candidate : candidates) {
        if (candidate > static_cast<uint64_t>(got)) {
            continue;
        }
        if (CanDeserializeInode(buf.data(), static_cast<size_t>(candidate))) {
            return candidate;
        }
    }
    return 0;
}

class SystemTester {
public:
    bool Init() {
        InitSimState();
        if (!FLAGS_mount_point.empty() && !fs::exists(FLAGS_mount_point)) {
            PrintOutputHeader();
            std::cout << u8"??????????????" << FLAGS_mount_point << "\n";
        }
        return true;
    }

    void RunMenu() {
        while (true) {
            PrintMenu();
            std::string choice = PromptLine(u8"???????? q ????");
            if (choice == "q" || choice == "Q") {
                break;
            }
            HandleMenu(choice);
        }
    }

private:
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
                PrintOutputHeader();
                std::cout << u8"????????" << FLAGS_start_file << "\n";
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
            PrintOutputHeader();
            std::cout << u8"??? inode ???????? --inode_dir?\n";
            return false;
        }

        uint64_t processed = 0;
        uint64_t failed = 0;
        auto start = std::chrono::steady_clock::now();

        while (processed < count && current_bin_file_idx_ < bin_files_.size()) {
            const auto& path = bin_files_[current_bin_file_idx_];
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) {
                PrintOutputHeader();
                std::cout << u8"???? inode ???" << path.string() << "\n";
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

            const uint64_t slot_size = DetectSlotSize(in, static_cast<uint64_t>(total_bytes));
            if (slot_size == 0) {
                PrintOutputHeader();
                std::cout << u8"???? inode ?????????" << path.string() << "\n";
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
            in.clear();
            in.seekg(static_cast<std::streamoff>(current_file_offset_ * slot_size), std::ios::beg);

            std::vector<uint8_t> slot(static_cast<size_t>(slot_size));
            while (processed < count && current_file_offset_ < total_slots) {
                in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot_size));
                if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
                    current_file_offset_ = total_slots;
                    PrintOutputHeader();
                    std::cout << u8"?????????????????????" << path.filename().string() << "\n";
                    break;
                }
                size_t off = 0;
                Inode inode;
                if (!Inode::deserialize(slot.data(), off, inode, static_cast<size_t>(slot_size))) {
                    ++sim_stats_.failed;
                    ++failed;
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
        PrintOutputHeader();
        std::cout << u8"?????? " << processed << u8" ????\n";
        std::cout << u8"?????" << elapsed / 1000.0 << u8" ??\n";
        std::cout << u8"??????????" << sim_stats_.inodes
                  << u8"????????" << FormatBytes(sim_stats_.bytes) << "\n";
        std::cout << u8"?????????" << failed << "\n";
        if (sim_stats_.inodes == 0 && failed > 0) {
            std::cout << u8"????????????????? inode ????????????\n";
        }
        return true;
    }

    void SimCountFileNum() {
        if (sim_stats_.inodes == 0) {
            PrintOutputHeader();
            std::cout << u8"???????????\n";
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
        PrintOutputHeader();
        std::cout << u8"???????????" << sim_stats_.inodes << "\n";
        std::cout << u8"????????" << FormatBytes(static_cast<uint64_t>(avg)) << "\n";
        std::cout << u8"??????????" << used_nodes << "\n";
    }

    void SimQueryFile(const std::string& path) {
        size_t h = std::hash<std::string>{}(path);
        uint64_t size = static_cast<uint64_t>(h % (100ULL * 1024 * 1024));
        if (size < 4096) size += 4096;
        double delay_ms = (static_cast<double>(size) / (100.0 * 1024 * 1024)) * 10.0;
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay_ms)));
        PrintOutputHeader();
        std::cout << u8"SimTrue=1 ??????????\n";
        std::cout << u8"??=" << path
                  << u8"?????=" << FormatBytes(size)
                  << u8"?????=" << delay_ms << "ms\n";
    }

    void SimWriteFile(const std::string& source_path) {
        std::error_code ec;
        uint64_t size = fs::file_size(source_path, ec);
        if (ec) {
            PrintOutputHeader();
            std::cout << u8"????????" << ec.message() << "\n";
            return;
        }
        double seconds = static_cast<double>(size) / (50.0 * 1024 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(seconds * 1000)));
        std::string node_id = sim_nodes_.empty() ? "none" : sim_nodes_[size % sim_nodes_.size()].node_id;
        PrintOutputHeader();
        std::cout << u8"SimTrue=1 ???????\n";
        std::cout << u8"????=" << FormatBytes(size)
                  << u8"???????ID=" << node_id << "\n";
    }

    void RealQueryFile(const std::string& path) {
        std::string real_path = MakeMountedPath(path);
        struct stat st;
        if (::stat(real_path.c_str(), &st) != 0) {
            PrintOutputHeader();
            std::cout << u8"[ERROR] ???????" << real_path
                      << u8"???=" << std::strerror(errno) << "\n";
            return;
        }
        PrintOutputHeader();
        std::cout << u8"==== ???? ====\n";
        std::cout << u8"SimTrue=0 ???????\n";
        std::cout << u8"??=" << real_path
                  << u8"???=" << st.st_size << u8" bytes"
                  << u8"???=" << st.st_mode << "\n";
    }

    void RealWriteContent(const std::string& dest_path, const std::string& content) {
        std::string real_dest = MakeMountedPath(dest_path);
        int out_fd = ::open(real_dest.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (out_fd < 0) {
            PrintOutputHeader();
            std::cout << u8"[ERROR] ?????????" << real_dest
                      << u8"???=" << std::strerror(errno) << "\n";
            return;
        }
        size_t offset = 0;
        const size_t total = content.size();
        while (offset < total) {
            ssize_t n = ::write(out_fd, content.data() + offset, total - offset);
            if (n <= 0) {
                PrintOutputHeader();
                std::cout << u8"[ERROR] ???????=" << std::strerror(errno) << "\n";
                ::close(out_fd);
                return;
            }
            offset += static_cast<size_t>(n);
        }
        ::close(out_fd);
        PrintOutputHeader();
        std::cout << u8"==== ???? ====\n";
        std::cout << u8"SimTrue=0 ???????\n";
        std::cout << u8"????=" << real_dest
                  << u8"??????=" << total << "\n";
    }

    void RealReadFile(const std::string& path) {
        std::string real_path = MakeMountedPath(path);
        int fd = ::open(real_path.c_str(), O_RDONLY);
        if (fd < 0) {
            PrintOutputHeader();
            std::cout << u8"[ERROR] ?????" << real_path
                      << u8"???=" << std::strerror(errno) << "\n";
            return;
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            PrintOutputHeader();
            std::cout << u8"[ERROR] ?????????" << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        const uint64_t max_read = 4ULL * 1024 * 1024;
        uint64_t to_read = static_cast<uint64_t>(st.st_size);
        bool truncated = false;
        if (to_read > max_read) {
            to_read = max_read;
            truncated = true;
        }
        std::vector<char> buf(static_cast<size_t>(to_read));
        ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            PrintOutputHeader();
            std::cout << u8"[ERROR] ???????=" << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        ::close(fd);
        PrintOutputHeader();
        std::cout << u8"==== ???? ====\n";
        std::cout << u8"SimTrue=0 ???????\n";
        std::cout << u8"??=" << real_path << u8"??????=" << n;
        if (truncated) {
            std::cout << u8"??????????";
        }
        std::cout << "\n";
        if (n > 0) {
            std::string out(buf.data(), buf.data() + n);
            std::cout << u8"???\n" << out << "\n";
        }
    }

    void PrintMenu() {
        std::cout << "\n" << kMenuSeparator << "\n";
        std::cout << u8"========== ZBSystemTest ?? ==========\n";
        std::cout << u8"1) ????????\n";
        std::cout << u8"2) ??????????\n";
        std::cout << u8"3) ????????\n";
        std::cout << u8"4) ????????\n";
        std::cout << u8"5) ?????????\n";
        std::cout << u8"6) ??????????\n";
        std::cout << u8"7) ???????????\n";
        std::cout << u8"8) ??????????\n";
        std::cout << u8"9) ???????POSIX?\n";
        std::cout << u8"10) ???????POSIX?????+???\n";
        std::cout << u8"11) ???????POSIX??????\n";
        std::cout << u8"q) ??\n";
        std::cout << u8"??????" << FLAGS_mount_point << "\n";
        std::cout << kMenuSeparator << "\n";
    }

    void CountTotalCapacityStorage() {
        uint64_t ssd = 0;
        uint64_t hdd = 0;
        for (const auto& node : sim_nodes_) {
            for (const auto& dev : node.ssd_devices) ssd += dev.capacity;
            for (const auto& dev : node.hdd_devices) hdd += dev.capacity;
        }
        uint64_t optical = 0;
        PrintOutputHeader();
        std::cout << u8"??????" << FormatBytes(ssd) << "\n";
        std::cout << u8"?????" << FormatBytes(hdd) << "\n";
        std::cout << u8"??????" << FormatBytes(optical) << "\n";
        std::cout << u8"????" << FormatBytes(ssd + hdd + optical) << "\n";
    }

    void CountStorageNodeNum() {
        uint64_t hdd_nodes = 0;
        uint64_t mix_nodes = 0;
        for (const auto& node : sim_nodes_) {
            if (node.type == 1) {
                ++hdd_nodes;
            } else if (node.type == 2) {
                ++mix_nodes;
            }
        }
        uint64_t optical_nodes = 0;
        PrintOutputHeader();
        std::cout << u8"???????" << hdd_nodes << "\n";
        std::cout << u8"???????" << mix_nodes << "\n";
        std::cout << u8"????????" << optical_nodes << "\n";
    }

    void CountDiscLibNum() {
        PrintOutputHeader();
        std::cout << u8"??????0\n";
    }

    void CountDiscNum() {
        PrintOutputHeader();
        std::cout << u8"?????0\n";
    }

    void HandleMenu(const std::string& choice) {
        if (choice == "1") {
            uint64_t count = PromptUint64(u8"????????????");
            SimBackup(count);
            return;
        }
        if (choice == "2") {
            SimCountFileNum();
            return;
        }
        if (choice == "3") {
            std::string path = PromptLine(u8"????????????? /path/file??");
            if (!path.empty()) {
                SimQueryFile(path);
            }
            return;
        }
        if (choice == "4") {
            std::string src = PromptLine(u8"???????????");
            if (!src.empty()) {
                SimWriteFile(src);
            }
            return;
        }
        if (choice == "5") {
            CountTotalCapacityStorage();
            return;
        }
        if (choice == "6") {
            CountStorageNodeNum();
            return;
        }
        if (choice == "7") {
            CountDiscLibNum();
            return;
        }
        if (choice == "8") {
            CountDiscNum();
            return;
        }
        if (choice == "9") {
            std::string path = PromptLine(u8"?????????????????? /hello.txt??");
            if (!path.empty()) {
                RealQueryFile(path);
            }
            return;
        }
        if (choice == "10") {
            std::string dest = PromptLine(u8"?????????????? /hello.txt??");
            if (dest.empty()) return;
            std::string content = PromptLine(u8"??????????????");
            RealWriteContent(dest, content);
            return;
        }
        if (choice == "11") {
            std::string path = PromptLine(u8"?????????????????? /hello.txt??");
            if (path.empty()) return;
            RealReadFile(path);
            return;
        }
        PrintOutputHeader();
        std::cout << u8"?????" << choice << "\n";
    }

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

    tester.RunMenu();
    return 0;
}
