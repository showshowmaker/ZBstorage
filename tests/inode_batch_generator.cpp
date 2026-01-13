#include <iostream>
#include <filesystem>
#include <string>
#include <vector>
#include <cstdlib>
#include <random>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <sstream>

#include "../src/mds/inode/InodeStorage.h"

namespace fs = std::filesystem;

struct CmdOptions {
    size_t batch_size = 1'000'000;  // 每个文件包含的 inode 数
    size_t batch_count = 1;         // 总共生成多少个文件
    uint64_t starting_inode = 0;    // 起始 inode 号
    std::string output_dir = "/mnt/md0/inode";
    uint32_t seed = 0;              // 0 表示随机
    bool verbose = true;
};

void print_usage(const char* prog) {
    std::cout << "用法: " << prog << " [选项]\n"
              << "  --batch-size <N>    每个批次的 inode 数 (默认 1'000'000)\n"
              << "  --batches <N>       批次数，决定生成的文件数量 (默认 1)\n"
              << "  --start-ino <N>     起始 inode 号 (默认 0)\n"
              << "  --output <PATH>     输出目录 (默认 /mnt/md0/inode)\n"
              << "  --seed <N>          固定随机种子 (默认 0 -> 随机)\n"
              << "  --quiet             关闭详细日志\n"
              << "  --help              显示本帮助\n";
}

bool parse_args(int argc, char** argv, CmdOptions& opts) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        } else if (arg == "--batch-size" && i + 1 < argc) {
            opts.batch_size = std::stoull(argv[++i]);
        } else if (arg == "--batches" && i + 1 < argc) {
            opts.batch_count = std::stoull(argv[++i]);
        } else if (arg == "--start-ino" && i + 1 < argc) {
            opts.starting_inode = std::stoull(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            opts.output_dir = argv[++i];
        } else if (arg == "--seed" && i + 1 < argc) {
            opts.seed = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--quiet") {
            opts.verbose = false;
        } else {
            std::cerr << "未知参数: " << arg << "\n";
            return false;
        }
    }
    if (opts.batch_size == 0) {
        opts.batch_size = 1'000'000;
    }
    if (opts.batch_count == 0) {
        opts.batch_count = 1;
    }
    return true;
}

std::string node_type_to_string(uint8_t type) {
    switch (type & 0x03) {
        case 0: return "SSD";
        case 1: return "HDD";
        case 2: return "Mix";
        default: return "Reserved";
    }
}

std::string format_digest(const std::vector<uint8_t>& digest) {
    if (digest.empty()) return "<empty>";
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    size_t preview = std::min<size_t>(digest.size(), 8);
    for (size_t i = 0; i < preview; ++i) {
        oss << std::setw(2) << static_cast<int>(digest[i]);
    }
    if (digest.size() > preview) {
        oss << "...";
    }
    return oss.str();
}

std::string format_timestamp(const InodeTimestamp& ts) {
    std::ostringstream oss;
    oss << (2000 + static_cast<int>(ts.year)) << "-"
        << std::setw(2) << std::setfill('0') << static_cast<int>(ts.month) << "-"
        << std::setw(2) << static_cast<int>(ts.day) << " "
        << std::setw(2) << static_cast<int>(ts.hour) << ":"
        << std::setw(2) << static_cast<int>(ts.minute);
    return oss.str();
}

void print_sample_inodes(const std::string& file, size_t sample_count) {
    const size_t slot_size = InodeStorage::INODE_DISK_SLOT_SIZE;
    std::ifstream in(file, std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "无法打开批量文件用于样例打印: " << file << std::endl;
        return;
    }

    in.seekg(0, std::ios::end);
    std::streamoff total_bytes = in.tellg();
    in.seekg(0, std::ios::beg);
    size_t available = static_cast<size_t>(total_bytes / static_cast<std::streamoff>(slot_size));
    size_t to_print = std::min(sample_count, available);
    if (to_print == 0) {
        std::cout << "[Sample] 文件为空: " << file << std::endl;
        return;
    }

    std::vector<uint8_t> slot(slot_size);
    std::cout << "[Sample] 展示文件 " << file << " 的前 " << to_print << " 条" << std::endl;
    for (size_t i = 0; i < to_print; ++i) {
        in.read(reinterpret_cast<char*>(slot.data()), slot_size);
        if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
            std::cerr << "读取槽失败，位置 " << i << std::endl;
            break;
        }
        size_t offset = 0;
        Inode inode;
        if (!Inode::deserialize(slot.data(), offset, inode, slot_size)) {
            std::cerr << "槽 " << i << " 反序列化失败" << std::endl;
            break;
        }
        std::cout << "  #" << i << " (inode=" << inode.inode << ")" << std::endl;
        std::cout << "    location: node=" << inode.location_id.fields.node_id
                  << " type=" << node_type_to_string(inode.location_id.fields.node_type)
                  << " raw=" << inode.location_id.raw << std::endl;
        std::cout << "    namespace_id=" << inode.getNamespaceId() << std::endl;
        std::cout << "    block_id=" << inode.block_id
                  << " filename_len=" << static_cast<int>(inode.filename_len)
                  << " digest_len=" << static_cast<int>(inode.digest_len) << std::endl;
        std::cout << "    filename=" << inode.filename << std::endl;
        std::cout << "    digest=" << format_digest(inode.digest) << std::endl;
        std::cout << "    file_mode: type=" << inode.file_mode.fields.file_type
                  << " perm=0" << std::oct << inode.file_mode.fields.file_perm << std::dec
                  << " raw=" << inode.file_mode.raw << std::endl;
        std::cout << "    file_size: unit=" << inode.file_size.fields.size_unit
                  << " raw_value=" << inode.file_size.fields.file_size
                  << " bytes=" << inode.getFileSize() << std::endl;
        std::cout << "    timestamps: fm=" << format_timestamp(inode.fm_time)
                  << " fa=" << format_timestamp(inode.fa_time)
                  << " fc=" << format_timestamp(inode.fc_time)
                  << " im=" << format_timestamp(inode.im_time) << std::endl;
        std::cout << "    volume_id=" << inode.getVolumeUUID() << std::endl;
        std::cout << "    blocks total=" << inode.block_count()
                  << " segments=" << inode.block_segments.size() << std::endl;
        size_t seg_preview = std::min<size_t>(inode.block_segments.size(), 3);
        for (size_t s = 0; s < seg_preview; ++s) {
            const auto& seg = inode.block_segments[s];
            std::cout << "      seg#" << s
                      << " logical=" << seg.logical_start
                      << " start_block=" << seg.start_block
                      << " count=" << seg.block_count << std::endl;
        }
        if (inode.block_segments.size() > seg_preview) {
            std::cout << "      ..." << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    CmdOptions opts;
    if (!parse_args(argc, argv, opts)) {
        print_usage(argv[0]);
        return 1;
    }

    fs::path out_dir(opts.output_dir);
    try {
        fs::create_directories(out_dir);
    } catch (const std::exception& ex) {
        std::cerr << "创建输出目录失败: " << ex.what() << std::endl;
        return 1;
    }

    uint64_t current_inode = opts.starting_inode;
    uint32_t current_seed = opts.seed;
    if (current_seed == 0) {
        std::random_device rd;
        current_seed = rd();
    }

    for (size_t batch = 0; batch < opts.batch_count; ++batch) {
        InodeStorage::BatchGenerationConfig cfg;
        cfg.batch_size = opts.batch_size;
        cfg.starting_inode = current_inode;
        cfg.output_file = (out_dir / ("inode_batch_" + std::to_string(batch) + ".bin")).string();
        cfg.random_seed = current_seed + static_cast<uint32_t>(batch * 1337);
        cfg.verbose = opts.verbose;
        cfg.node_distribution = {
            {1, 0, 0.5}, // 同一节点编号，标记为 SSD 类型
            {1, 1, 0.3}, // 同一节点编号，标记为 HDD 类型
            {1, 2, 0.2}  // 同一节点编号，标记为 Mix 类型
        };

        try {
            if (!InodeStorage::generate_metadata_batch(cfg)) {
                std::cerr << "批次 " << batch << " 生成失败" << std::endl;
                return 1;
            }
        } catch (const std::exception& ex) {
            std::cerr << "批次 " << batch << " 抛出异常: " << ex.what() << std::endl;
            return 1;
        }

        if (opts.verbose) {
            std::cout << "已生成: " << cfg.output_file << std::endl;
        }

        print_sample_inodes(cfg.output_file, 3);

        current_inode += opts.batch_size;
    }

    if (opts.verbose) {
        std::cout << "全部批次完成，共生成 " << opts.batch_count
                  << " 个文件，目录: " << out_dir << std::endl;
    }

    return 0;
}
