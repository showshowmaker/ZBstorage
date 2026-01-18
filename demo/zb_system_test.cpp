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

std::string PromptLine(const std::string& tip) {
    std::cout << tip;
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
            std::cout << u8"输入无效，请重新输入。\n";
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

class SystemTester {
public:
    bool Init() {
        InitSimState();
        if (!FLAGS_mount_point.empty() && !fs::exists(FLAGS_mount_point)) {
            std::cout << u8"提示：挂载点不存在或未挂载：" << FLAGS_mount_point << "\n";
        }
        return true;
    }

    void RunMenu() {
        while (true) {
            PrintMenu();
            std::string choice = PromptLine(u8"请选择操作（输入 q 退出）：");
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
                std::cout << u8"起始文件未找到：" << FLAGS_start_file << "\n";
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
            std::cout << u8"找不到 inode 批量文件，请设置 --inode_dir。\n";
            return false;
        }

        const uint64_t slot_size = InodeStorage::INODE_DISK_SLOT_SIZE;
        uint64_t processed = 0;
        auto start = std::chrono::steady_clock::now();

        while (processed < count && current_bin_file_idx_ < bin_files_.size()) {
            const auto& path = bin_files_[current_bin_file_idx_];
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) {
                std::cout << u8"无法打开 inode 文件：" << path.string() << "\n";
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
        std::cout << u8"已经成功备份 " << processed << u8" 个文件。\n";
        std::cout << u8"执行时间：" << elapsed / 1000.0 << u8" 秒。\n";
        std::cout << u8"当前已处理文件数量：" << sim_stats_.inodes
                  << u8"，累计模拟写入：" << FormatBytes(sim_stats_.bytes) << "\n";
        return true;
    }

    void SimCountFileNum() {
        if (sim_stats_.inodes == 0) {
            std::cout << u8"当前没有已处理的文件。\n";
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
        std::cout << u8"系统中包含文件数量为：" << sim_stats_.inodes << "\n";
        std::cout << u8"平均文件大小为：" << FormatBytes(static_cast<uint64_t>(avg)) << "\n";
        std::cout << u8"使用存储节点数量为：" << used_nodes << "\n";
    }

    void SimQueryFile(const std::string& path) {
        size_t h = std::hash<std::string>{}(path);
        uint64_t size = static_cast<uint64_t>(h % (100ULL * 1024 * 1024));
        if (size < 4096) size += 4096;
        double delay_ms = (static_cast<double>(size) / (100.0 * 1024 * 1024)) * 10.0;
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay_ms)));
        std::cout << u8"SimTrue=1 表示从仿真系统查询。\n";
        std::cout << u8"文件=" << path
                  << u8"，模拟大小=" << FormatBytes(size)
                  << u8"，模拟耗时=" << delay_ms << "ms\n";
    }

    void SimWriteFile(const std::string& source_path) {
        std::error_code ec;
        uint64_t size = fs::file_size(source_path, ec);
        if (ec) {
            std::cout << u8"读取源文件失败：" << ec.message() << "\n";
            return;
        }
        double seconds = static_cast<double>(size) / (50.0 * 1024 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(seconds * 1000)));
        std::string node_id = sim_nodes_.empty() ? "none" : sim_nodes_[size % sim_nodes_.size()].node_id;
        std::cout << u8"SimTrue=1 表示仿真写入。\n";
        std::cout << u8"写入大小=" << FormatBytes(size)
                  << u8"，写入存储节点ID=" << node_id << "\n";
    }

    void RealQueryFile(const std::string& path) {
        std::string real_path = MakeMountedPath(path);
        struct stat st;
        if (::stat(real_path.c_str(), &st) != 0) {
            std::cout << u8"真实查询失败：" << real_path << u8"，错误=" << std::strerror(errno) << "\n";
            return;
        }
        std::cout << u8"SimTrue=0 表示真实查询。\n";
        std::cout << u8"文件=" << real_path
                  << u8"，大小=" << st.st_size << u8" bytes"
                  << u8"，权限=" << st.st_mode << "\n";
    }

    void RealWriteFile(const std::string& source_path, const std::string& dest_path) {
        std::string real_dest = MakeMountedPath(dest_path);
        int in_fd = ::open(source_path.c_str(), O_RDONLY);
        if (in_fd < 0) {
            std::cout << u8"打开源文件失败：" << source_path << u8"，错误=" << std::strerror(errno) << "\n";
            return;
        }
        int out_fd = ::open(real_dest.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (out_fd < 0) {
            std::cout << u8"打开目标文件失败：" << real_dest << u8"，错误=" << std::strerror(errno) << "\n";
            ::close(in_fd);
            return;
        }
        const size_t kBuf = 1 << 20;
        std::vector<char> buf(kBuf);
        ssize_t read_bytes = 0;
        uint64_t total = 0;
        while ((read_bytes = ::read(in_fd, buf.data(), buf.size())) > 0) {
            size_t offset = 0;
            while (offset < static_cast<size_t>(read_bytes)) {
                ssize_t n = ::write(out_fd, buf.data() + offset, static_cast<size_t>(read_bytes) - offset);
                if (n <= 0) {
                    std::cout << u8"写入失败，错误=" << std::strerror(errno) << "\n";
                    ::close(in_fd);
                    ::close(out_fd);
                    return;
                }
                offset += static_cast<size_t>(n);
                total += static_cast<uint64_t>(n);
            }
        }
        ::close(in_fd);
        ::close(out_fd);
        std::cout << u8"SimTrue=0 表示真实写入。\n";
        std::cout << u8"目标文件=" << real_dest
                  << u8"，写入字节数=" << total << "\n";
    }

    void RealReadFile(const std::string& path, uint64_t max_bytes) {
        std::string real_path = MakeMountedPath(path);
        int fd = ::open(real_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cout << u8"读取失败：" << real_path << u8"，错误=" << std::strerror(errno) << "\n";
            return;
        }
        std::vector<char> buf(static_cast<size_t>(max_bytes));
        ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            std::cout << u8"读取失败，错误=" << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        ::close(fd);
        std::cout << u8"SimTrue=0 表示真实读取。\n";
        std::cout << u8"文件=" << real_path << u8"，读取字节数=" << n << "\n";
        if (n > 0) {
            std::string out(buf.data(), buf.data() + n);
            std::cout << u8"内容预览：\n" << out << "\n";
        }
    }

    void PrintMenu() {
        std::cout << "\n";
        std::cout << u8"========== ZBSystemTest 菜单 ==========\n";
        std::cout << u8"1) 批量备份（仿真）\n";
        std::cout << u8"2) 统计文件数量（仿真）\n";
        std::cout << u8"3) 查询文件（仿真）\n";
        std::cout << u8"4) 写入文件（仿真）\n";
        std::cout << u8"5) 统计总容量（仿真）\n";
        std::cout << u8"6) 统计节点数量（仿真）\n";
        std::cout << u8"7) 统计光盘库数量（仿真）\n";
        std::cout << u8"8) 统计光盘数量（仿真）\n";
        std::cout << u8"9) 真实查询文件（POSIX）\n";
        std::cout << u8"10) 真实写入文件（POSIX）\n";
        std::cout << u8"11) 真实读取文件（POSIX）\n";
        std::cout << u8"q) 退出\n";
        std::cout << u8"当前挂载点：" << FLAGS_mount_point << "\n";
    }

    void CountTotalCapacityStorage() {
        uint64_t ssd = 0;
        uint64_t hdd = 0;
        for (const auto& node : sim_nodes_) {
            for (const auto& dev : node.ssd_devices) ssd += dev.capacity;
            for (const auto& dev : node.hdd_devices) hdd += dev.capacity;
        }
        uint64_t optical = 0;
        std::cout << u8"固态盘容量：" << FormatBytes(ssd) << "\n";
        std::cout << u8"磁盘容量：" << FormatBytes(hdd) << "\n";
        std::cout << u8"光存储容量：" << FormatBytes(optical) << "\n";
        std::cout << u8"总容量：" << FormatBytes(ssd + hdd + optical) << "\n";
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
        std::cout << u8"磁盘节点数量：" << hdd_nodes << "\n";
        std::cout << u8"混合节点数量：" << mix_nodes << "\n";
        std::cout << u8"光盘库节点数量：" << optical_nodes << "\n";
    }

    void CountDiscLibNum() {
        std::cout << u8"光盘库数量：0\n";
    }

    void CountDiscNum() {
        std::cout << u8"光盘数量：0\n";
    }

    void HandleMenu(const std::string& choice) {
        if (choice == "1") {
            uint64_t count = PromptUint64(u8"请输入要备份的文件数量：");
            SimBackup(count);
            return;
        }
        if (choice == "2") {
            SimCountFileNum();
            return;
        }
        if (choice == "3") {
            std::string path = PromptLine(u8"请输入要查询的文件路径（如 /path/file）：");
            if (!path.empty()) {
                SimQueryFile(path);
            }
            return;
        }
        if (choice == "4") {
            std::string src = PromptLine(u8"请输入本地源文件路径：");
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
            std::string path = PromptLine(u8"请输入要查询的文件路径（挂载点内，如 /hello.txt）：");
            if (!path.empty()) {
                RealQueryFile(path);
            }
            return;
        }
        if (choice == "10") {
            std::string src = PromptLine(u8"请输入本地源文件路径：");
            if (src.empty()) return;
            std::string dest = PromptLine(u8"请输入写入路径（挂载点内路径，如 /hello.txt，留空默认同名）：");
            if (dest.empty()) {
                dest = "/" + fs::path(src).filename().string();
            }
            RealWriteFile(src, dest);
            return;
        }
        if (choice == "11") {
            std::string path = PromptLine(u8"请输入要读取的文件路径（挂载点内，如 /hello.txt）：");
            if (path.empty()) return;
            uint64_t max_bytes = PromptUint64(u8"请输入读取上限字节数（默认 4096）：", 4096);
            RealReadFile(path, max_bytes);
            return;
        }
        std::cout << u8"未知选项：" << choice << "\n";
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