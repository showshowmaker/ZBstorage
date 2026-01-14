#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../src/mds/inode/InodeStorage.h"

namespace fs = std::filesystem;

namespace {

struct Options {
    std::string inode_dir;
    std::string json_log;
    uint64_t max_inodes{0};
    std::string start_file;
    uint64_t start_index{0};
    uint32_t ssd_nodes{0};
    uint32_t hdd_nodes{0};
    uint32_t mix_nodes{0};
    uint32_t ssd_devices_per_node{1};
    uint32_t hdd_devices_per_node{1};
    uint64_t ssd_capacity_bytes{0};
    uint64_t hdd_capacity_bytes{0};
    uint32_t report_interval_sec{1};
    uint32_t print_limit_nodes{0};
};

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
    std::string current_file;
    uint64_t current_index{0};
    std::string current_node;
};

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " --inode_dir <dir> --json_log <path> [options]\n"
              << "  --inode_dir <dir>          inode batch directory\n"
              << "  --json_log <path>          json log output file\n"
              << "  --ssd_nodes <N>            SSD node count\n"
              << "  --hdd_nodes <N>            HDD node count\n"
              << "  --mix_nodes <N>            Mix node count\n"
              << "  --ssd_devices_per_node <N> SSD devices per SSD/Mix node (default 1)\n"
              << "  --hdd_devices_per_node <N> HDD devices per HDD/Mix node (default 1)\n"
              << "  --ssd_capacity_bytes <N>   SSD device capacity bytes\n"
              << "  --hdd_capacity_bytes <N>   HDD device capacity bytes\n"
              << "  --report_interval_sec <N>  report interval in seconds (default 1)\n"
              << "  --max_inodes <N>           max inodes to process (0 = all)\n"
              << "  --print_limit_nodes <N>    limit node prints per report (0 = all)\n"
              << "  --start_file <name>        start from this inode file (name or full path)\n"
              << "  --start_index <N>          start inode index within start_file (default 0)\n";
}

bool ParseArgs(int argc, char** argv, Options& opts) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--inode_dir" && i + 1 < argc) {
            opts.inode_dir = argv[++i];
        } else if (arg == "--json_log" && i + 1 < argc) {
            opts.json_log = argv[++i];
        } else if (arg == "--ssd_nodes" && i + 1 < argc) {
            opts.ssd_nodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--hdd_nodes" && i + 1 < argc) {
            opts.hdd_nodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--mix_nodes" && i + 1 < argc) {
            opts.mix_nodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--ssd_devices_per_node" && i + 1 < argc) {
            opts.ssd_devices_per_node = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--hdd_devices_per_node" && i + 1 < argc) {
            opts.hdd_devices_per_node = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--ssd_capacity_bytes" && i + 1 < argc) {
            opts.ssd_capacity_bytes = std::stoull(argv[++i]);
        } else if (arg == "--hdd_capacity_bytes" && i + 1 < argc) {
            opts.hdd_capacity_bytes = std::stoull(argv[++i]);
        } else if (arg == "--report_interval_sec" && i + 1 < argc) {
            opts.report_interval_sec = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--max_inodes" && i + 1 < argc) {
            opts.max_inodes = std::stoull(argv[++i]);
        } else if (arg == "--print_limit_nodes" && i + 1 < argc) {
            opts.print_limit_nodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--start_file" && i + 1 < argc) {
            opts.start_file = argv[++i];
        } else if (arg == "--start_index" && i + 1 < argc) {
            opts.start_index = std::stoull(argv[++i]);
        } else if (arg == "--help") {
            PrintUsage(argv[0]);
            return false;
        } else {
            std::cerr << "Unknown arg: " << arg << "\n";
            return false;
        }
    }
    if (opts.inode_dir.empty() || opts.json_log.empty()) {
        std::cerr << "--inode_dir and --json_log are required\n";
        return false;
    }
    if (opts.ssd_capacity_bytes == 0 || opts.hdd_capacity_bytes == 0) {
        std::cerr << "--ssd_capacity_bytes and --hdd_capacity_bytes are required\n";
        return false;
    }
    return true;
}

std::string NodeTypeName(uint8_t type) {
    switch (type & 0x03) {
        case 0: return "SSD";
        case 1: return "HDD";
        case 2: return "Mix";
        default: return "Reserved";
    }
}


bool MatchesStartFile(const fs::path& path, const std::string& start) {
    if (start.empty()) return true;
    if (path.string() == start) return true;
    return path.filename().string() == start;
}

std::string JsonEscape(const std::string& input) {
    std::ostringstream oss;
    for (char c : input) {
        switch (c) {
            case '\\': oss << "\\\\"; break;
            case '"': oss << "\\\""; break;
            case '\n': oss << "\\n"; break;
            case '\r': oss << "\\r"; break;
            case '\t': oss << "\\t"; break;
            default: oss << c; break;
        }
    }
    return oss.str();
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

std::string FormatBytesSigned(int64_t bytes) {
    if (bytes < 0) {
        return "-" + FormatBytes(static_cast<uint64_t>(-bytes));
    }
    return "+" + FormatBytes(static_cast<uint64_t>(bytes));
}

void PrintReport(const std::vector<NodeState>& nodes,
                 const Stats& stats,
                 uint32_t limit_nodes,
                 std::unordered_map<std::string, uint64_t>& last_used) {
    std::cout << u8"[\u7edf\u8ba1] inode=" << stats.inodes
              << u8" \u5b57\u8282=" << FormatBytes(stats.bytes)
              << u8" \u5931\u8d25=" << stats.failed
              << u8" \u7f3a\u5931\u8282\u70b9=" << stats.missing_node
              << u8" \u5f53\u524d\u6587\u4ef6=" << stats.current_file
              << u8" \u7d22\u5f15=" << stats.current_index
              << u8" \u8282\u70b9=" << stats.current_node << "\n";

    uint32_t printed_nodes = 0;
    uint32_t printed_devices = 0;
    for (const auto& node : nodes) {
        bool node_printed = false;
        auto handle_device = [&](const DeviceState& dev) {
            const uint64_t free = dev.capacity > dev.used ? dev.capacity - dev.used : 0;
            const auto it = last_used.find(dev.device_id);
            const uint64_t prev = it == last_used.end() ? 0 : it->second;
            if (dev.used == prev) {
                return;
            }
            if (!node_printed) {
                if (limit_nodes > 0 && printed_nodes >= limit_nodes) {
                    return;
                }
                std::cout << u8"\u8282\u70b9 " << node.node_id
                          << " (" << NodeTypeName(node.type) << ")\n";
                node_printed = true;
                ++printed_nodes;
            }
            const int64_t delta = static_cast<int64_t>(dev.used) - static_cast<int64_t>(prev);
            std::cout << u8"  \u8bbe\u5907 " << dev.device_id
                      << u8" \u5df2\u7528=" << FormatBytes(dev.used)
                      << u8" \u53d8\u5316=" << FormatBytesSigned(delta)
                      << u8" \u5269\u4f59=" << FormatBytes(free)
                      << u8"\n";
            last_used[dev.device_id] = dev.used;
            ++printed_devices;
        };

        for (const auto& dev : node.ssd_devices) {
            handle_device(dev);
            if (limit_nodes > 0 && printed_nodes >= limit_nodes && node_printed) {
                break;
            }
        }
        if (limit_nodes > 0 && printed_nodes >= limit_nodes && node_printed) {
            continue;
        }
        for (const auto& dev : node.hdd_devices) {
            handle_device(dev);
            if (limit_nodes > 0 && printed_nodes >= limit_nodes && node_printed) {
                break;
            }
        }
    }
    if (printed_devices == 0) {
        std::cout << u8"[\u7edf\u8ba1] \u672c\u5468\u671f\u6ca1\u6709\u8bbe\u5907\u5bb9\u91cf\u53d8\u5316\n";
    }
}

void WriteJsonReport(std::ofstream& out, const std::vector<NodeState>& nodes, const Stats& stats) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    out << "{";
    out << "\"timestamp_ms\":" << ms << ",";
    out << "\"inodes\":" << stats.inodes << ",";
    out << "\"bytes\":" << stats.bytes << ",";
    out << "\"failed\":" << stats.failed << ",";
    out << "\"missing_node\":" << stats.missing_node << ",";
    out << "\"current_file\":\"" << JsonEscape(stats.current_file) << "\",";
    out << "\"current_index\":" << stats.current_index << ",";
    out << "\"current_node\":\"" << JsonEscape(stats.current_node) << "\",";
    out << "\"nodes\":[";
    for (size_t i = 0; i < nodes.size(); ++i) {
        const auto& node = nodes[i];
        out << "{";
        out << "\"node_id\":\"" << JsonEscape(node.node_id) << "\",";
        out << "\"type\":" << static_cast<int>(node.type) << ",";
        out << "\"devices\":[";
        bool first_dev = true;
        for (const auto& dev : node.ssd_devices) {
            if (!first_dev) out << ",";
            first_dev = false;
            uint64_t free = dev.capacity > dev.used ? dev.capacity - dev.used : 0;
            out << "{\"device_id\":\"" << JsonEscape(dev.device_id) << "\",";
            out << "\"used\":" << dev.used << ",";
            out << "\"free\":" << free << ",";
            out << "\"capacity\":" << dev.capacity << "}";
        }
        for (const auto& dev : node.hdd_devices) {
            if (!first_dev) out << ",";
            first_dev = false;
            uint64_t free = dev.capacity > dev.used ? dev.capacity - dev.used : 0;
            out << "{\"device_id\":\"" << JsonEscape(dev.device_id) << "\",";
            out << "\"used\":" << dev.used << ",";
            out << "\"free\":" << free << ",";
            out << "\"capacity\":" << dev.capacity << "}";
        }
        out << "]}";
        if (i + 1 < nodes.size()) out << ",";
    }
    out << "]}\n";
    out.flush();
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

} // namespace

int main(int argc, char** argv) {
    Options opts;
    if (!ParseArgs(argc, argv, opts)) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::vector<NodeState> nodes;
    nodes.reserve(opts.ssd_nodes + opts.hdd_nodes + opts.mix_nodes);

    for (uint32_t i = 0; i < opts.ssd_nodes; ++i) {
        NodeState node;
        node.node_id = "node_ssd_" + std::to_string(i);
        node.type = 0;
        for (uint32_t d = 0; d < opts.ssd_devices_per_node; ++d) {
            DeviceState dev;
            dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
            dev.capacity = opts.ssd_capacity_bytes;
            node.ssd_devices.push_back(std::move(dev));
        }
        nodes.push_back(std::move(node));
    }

    for (uint32_t i = 0; i < opts.hdd_nodes; ++i) {
        NodeState node;
        node.node_id = "node_hdd_" + std::to_string(i);
        node.type = 1;
        for (uint32_t d = 0; d < opts.hdd_devices_per_node; ++d) {
            DeviceState dev;
            dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
            dev.capacity = opts.hdd_capacity_bytes;
            node.hdd_devices.push_back(std::move(dev));
        }
        nodes.push_back(std::move(node));
    }

    for (uint32_t i = 0; i < opts.mix_nodes; ++i) {
        NodeState node;
        node.node_id = "node_mix_" + std::to_string(i);
        node.type = 2;
        for (uint32_t d = 0; d < opts.ssd_devices_per_node; ++d) {
            DeviceState dev;
            dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
            dev.capacity = opts.ssd_capacity_bytes;
            node.ssd_devices.push_back(std::move(dev));
        }
        for (uint32_t d = 0; d < opts.hdd_devices_per_node; ++d) {
            DeviceState dev;
            dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
            dev.capacity = opts.hdd_capacity_bytes;
            node.hdd_devices.push_back(std::move(dev));
        }
        nodes.push_back(std::move(node));
    }

    std::unordered_map<std::string, size_t> node_index;
    node_index.reserve(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {
        node_index[nodes[i].node_id] = i;
    }

    std::vector<fs::path> files;
    for (const auto& entry : fs::directory_iterator(opts.inode_dir)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() == ".bin") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    if (!opts.start_file.empty()) {
        bool found = false;
        for (const auto& path : files) {
            if (MatchesStartFile(path, opts.start_file)) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "Start file not found: " << opts.start_file << "\n";
            return 1;
        }
    }
    if (files.empty()) {
        std::cerr << "No inode batch files found in " << opts.inode_dir << "\n";
        return 1;
    }

    std::ofstream json_out(opts.json_log, std::ios::app);
    if (!json_out.is_open()) {
        std::cerr << "Failed to open json log: " << opts.json_log << "\n";
        return 1;
    }

    Stats stats;
    std::unordered_map<std::string, uint64_t> last_used;
    auto last_report = std::chrono::steady_clock::now();
    const uint64_t slot_size = InodeStorage::INODE_DISK_SLOT_SIZE;

    bool started = opts.start_file.empty();
    for (const auto& path : files) {
        if (!started) {
            if (!MatchesStartFile(path, opts.start_file)) {
                continue;
            }
            started = true;
        }
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Failed to open inode file: " << path.string() << "\n";
            continue;
        }
        in.seekg(0, std::ios::end);
        const std::streamoff total_bytes = in.tellg();
        if (total_bytes <= 0) {
            continue;
        }
        in.seekg(0, std::ios::beg);
        const uint64_t total_slots = static_cast<uint64_t>(total_bytes) / slot_size;

        uint64_t start_offset = 0;
        if (!opts.start_file.empty() && MatchesStartFile(path, opts.start_file)) {
            start_offset = opts.start_index;
            if (start_offset >= total_slots) {
                std::cerr << "Start index out of range for file: " << path.string() << "\n";
                return 1;
            }
        }

        std::vector<uint8_t> slot(static_cast<size_t>(slot_size));
        for (uint64_t idx = start_offset; idx < total_slots; ++idx) {
            in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot_size));
            if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
                break;
            }
            size_t off = 0;
            Inode inode;
            if (!Inode::deserialize(slot.data(), off, inode, slot_size)) {
                ++stats.failed;
                continue;
            }
            ++stats.inodes;

            const uint64_t bytes = inode.getFileSize();
            stats.bytes += bytes;
            stats.current_file = path.filename().string();
            stats.current_index = idx;

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

            stats.current_node = node_name;

            auto it = node_index.find(node_name);
            if (it == node_index.end()) {
                ++stats.missing_node;
            } else {
                auto& node = nodes[it->second];
                uint64_t remaining = bytes;
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
                    ++stats.failed;
                }
            }

            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= opts.report_interval_sec) {
                PrintReport(nodes, stats, opts.print_limit_nodes, last_used);
                WriteJsonReport(json_out, nodes, stats);
                last_report = now;
            }

            if (opts.max_inodes > 0 && stats.inodes >= opts.max_inodes) {
                break;
            }
        }

        if (opts.max_inodes > 0 && stats.inodes >= opts.max_inodes) {
            break;
        }
    }

    PrintReport(nodes, stats, opts.print_limit_nodes, last_used);
    WriteJsonReport(json_out, nodes, stats);
    return 0;
}
