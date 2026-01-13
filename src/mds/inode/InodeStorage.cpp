#include "InodeStorage.h"

#include <cstring>      // std::memcpy
#include <stdexcept>    // std::runtime_error
#include <iostream>     // 调试输出（可选）
#include <array>
#include <limits>
#include <random>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <chrono>

// --- InodeStorage 实现 ---

namespace {

enum class TemperatureClass { Hot = 0, Warm = 1, Cold = 2 };

struct AccessProfile {
    int min_days;
    int max_days;
};

uint64_t pick_size(const InodeStorage::SizeRange& range, std::mt19937_64& rng) {
    if (range.max_bytes <= range.min_bytes) {
        return range.min_bytes;
    }
    std::uniform_int_distribution<uint64_t> dist(range.min_bytes, range.max_bytes);
    return dist(rng);
}

void encode_size_fields(uint64_t bytes, uint16_t& unit, uint16_t& value) {
    static constexpr uint16_t MAX_FIELD = (1u << 14) - 1;
    unit = 0;
    uint64_t normalized = bytes;
    while (unit < 3 && normalized > MAX_FIELD) {
        normalized = (normalized + 1023) / 1024; // 1024 进制换算
        ++unit;
    }
    if (normalized > MAX_FIELD) {
        normalized = MAX_FIELD;
    }
    value = static_cast<uint16_t>(normalized);
}

std::string build_path_name(uint64_t ino,
                            TemperatureClass klass,
                            const InodeStorage::BatchGenerationConfig& cfg,
                            std::mt19937_64& rng) {
    const char* prefix = "cold";
    if (klass == TemperatureClass::Hot) {
        prefix = "hot";
    } else if (klass == TemperatureClass::Warm) {
        prefix = "warm";
    }

    std::string root = cfg.root_path.empty() ? "/dataset" : cfg.root_path;
    if (root.size() > 1 && root.back() == '/') {
        root.pop_back();
    }
    if (root.empty()) {
        root = "/dataset";
    } else if (root.front() != '/') {
        root = "/" + root;
    }

    size_t depth = cfg.dir_depth == 0 ? 1 : cfg.dir_depth;
    size_t fanout = cfg.dir_fanout == 0 ? 4 : cfg.dir_fanout;
    std::uniform_int_distribution<size_t> dir_dist(0, fanout - 1);

    std::ostringstream oss;
    oss << root;
    for (size_t level = 0; level < depth; ++level) {
        size_t idx = dir_dist(rng);
        oss << "/level" << level << '_' << idx;
    }
    oss << '/' << prefix << "_file_" << ino;
    return oss.str();
}

std::vector<uint8_t> build_digest(std::mt19937_64& rng, size_t length = 32) {
    std::vector<uint8_t> digest(length);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (auto& b : digest) {
        b = static_cast<uint8_t>(byte_dist(rng));
    }
    return digest;
}

std::vector<BlockSegment> build_segments(size_t total_blocks,
                                         size_t max_segments,
                                         const InodeStorage::NodeDistributionEntry& node,
                                         std::mt19937_64& rng) {
    if (total_blocks == 0) {
        total_blocks = 1;
    }
    size_t capped_segments = max_segments == 0 ? 1 : max_segments;
    std::uniform_int_distribution<size_t> seg_count_dist(1, capped_segments);
    size_t segment_count = std::min(total_blocks, seg_count_dist(rng));

    std::vector<BlockSegment> segments;
    segments.reserve(segment_count);

    size_t blocks_left = total_blocks;
    size_t logical_cursor = 0;
    size_t physical_cursor = static_cast<size_t>(node.node_id) * 1'000'000ULL;
    for (size_t s = 0; s < segment_count; ++s) {
        size_t remaining_segments = segment_count - s;
        size_t max_for_segment = blocks_left - (remaining_segments - 1);
        std::uniform_int_distribution<size_t> len_dist(1, max_for_segment);
        size_t len = len_dist(rng);
        segments.emplace_back(logical_cursor, physical_cursor, len);
        logical_cursor += len;
        physical_cursor += len + 3; // 空洞用于模拟碎片
        blocks_left -= len;
    }
    return segments;
}

TemperatureClass pick_temperature(const InodeStorage::BatchGenerationConfig& cfg,
                                   std::mt19937_64& rng) {
    std::vector<double> weights = {
        std::max(cfg.temp_ratio.hot, 0.0),
        std::max(cfg.temp_ratio.warm, 0.0),
        std::max(cfg.temp_ratio.cold, 0.0)
    };
    double sum = weights[0] + weights[1] + weights[2];
    if (sum <= 0.0) {
        weights = {0.2, 0.3, 0.5};
        sum = 1.0;
    }
    std::discrete_distribution<int> dist(weights.begin(), weights.end());
    return static_cast<TemperatureClass>(dist(rng));
}

const InodeStorage::SizeRange& range_for_temperature(
    const InodeStorage::BatchGenerationConfig& cfg,
    TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot: return cfg.hot_range;
        case TemperatureClass::Warm: return cfg.warm_range;
        default: return cfg.cold_range;
    }
}

std::string volume_for_temperature(TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot: return "vol_hot";
        case TemperatureClass::Warm: return "vol_warm";
        default: return "vol_cold";
    }
}

AccessProfile access_profile_for_temp(TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot:  return {0, 30};
        case TemperatureClass::Warm: return {60, 120};
        case TemperatureClass::Cold: return {180, 365};
        default: return {0, 30};
    }
}

uint32_t encode_year_offset(int full_year) {
    int offset = full_year - 2000;
    if (offset < 0) offset = 0;
    if (offset > 255) offset = 255;
    return static_cast<uint32_t>(offset);
}

#if defined(_WIN32)
InodeTimestamp make_timestamp(std::chrono::system_clock::time_point tp) {
    std::time_t raw = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_buf;
    localtime_s(&tm_buf, &raw);
#else
InodeTimestamp make_timestamp(std::chrono::system_clock::time_point tp) {
    std::time_t raw = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_buf;
    localtime_r(&raw, &tm_buf);
#endif
    InodeTimestamp ts;
    int full_year = tm_buf.tm_year + 1900;
    ts.year = encode_year_offset(full_year);
    ts.month = static_cast<uint32_t>(tm_buf.tm_mon + 1);
    ts.day = static_cast<uint32_t>(tm_buf.tm_mday);
    ts.hour = static_cast<uint32_t>(tm_buf.tm_hour);
    ts.minute = static_cast<uint32_t>(tm_buf.tm_min);
    return ts;
}

std::chrono::system_clock::time_point tp_days_ago(
    const std::chrono::system_clock::time_point& now_tp,
    int days,
    int hour_offset,
    int minute_offset) {
    auto total_hours = std::chrono::hours(days * 24 + hour_offset);
    auto total_minutes = std::chrono::minutes(minute_offset);
    return now_tp - total_hours - total_minutes;
}

void apply_temperature_timestamps(Inode& inode,
                                  TemperatureClass klass,
                                  std::mt19937_64& rng,
                                  const std::chrono::system_clock::time_point& now_tp) {
    auto profile = access_profile_for_temp(klass);
    std::uniform_int_distribution<int> day_dist(profile.min_days, profile.max_days);
    std::uniform_int_distribution<int> hour_dist(0, 23);
    std::uniform_int_distribution<int> minute_dist(0, 59);

    int access_days = day_dist(rng);
    auto fa_tp = tp_days_ago(now_tp, access_days, hour_dist(rng), minute_dist(rng));

    std::uniform_int_distribution<int> modify_extra(1, 14);
    int fm_days = access_days + modify_extra(rng);
    auto fm_tp = tp_days_ago(now_tp, fm_days, hour_dist(rng), minute_dist(rng));

    std::uniform_int_distribution<int> create_extra(15, 60);
    int fc_days = fm_days + create_extra(rng);
    auto fc_tp = tp_days_ago(now_tp, fc_days, hour_dist(rng), minute_dist(rng));

    inode.fa_time = make_timestamp(fa_tp);
    inode.fm_time = make_timestamp(fm_tp);
    inode.fc_time = make_timestamp(fc_tp);
    inode.im_time = inode.fm_time;
}

} // namespace

InodeStorage::InodeStorage(const std::string& path, bool create_new) {
    file_path = path;
    // 如果 create_new=true，则清空并以读写方式创建；否则以读写方式打开。
    std::ios_base::openmode mode = create_new
        ? (std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc)
        : (std::ios::in | std::ios::out | std::ios::binary);

    inode_file.open(file_path, mode);
    if (!inode_file.is_open()) {
        throw std::runtime_error("Failed to open inode file");
    }
}

InodeStorage::~InodeStorage() {
    if (inode_file.is_open()) inode_file.close();
}

bool InodeStorage::write_inode(uint64_t ino, const Inode& dinode) {
    std::lock_guard<std::mutex> lock(file_mutex);

    std::vector<uint8_t> serialized = dinode.serialize();
    if (serialized.size() > INODE_DISK_SLOT_SIZE) {
        throw std::runtime_error("serialize inode size > INODE_DISK_SLOT_SIZE");
    }

#ifdef inode_debug
    std::cout << "----debug-struct.h:-----" << std::endl;
    std::cout << "inode no:" << ino << std::endl;
    std::cout << "inode size : " << serialized.size() << std::endl;
#endif

    std::vector<uint8_t> disk_buffer(INODE_DISK_SLOT_SIZE, 0);
    std::memcpy(disk_buffer.data(), serialized.data(), serialized.size());
    size_t offset = ino * INODE_DISK_SLOT_SIZE;

    inode_file.seekp(offset);
    inode_file.write(reinterpret_cast<const char*>(disk_buffer.data()), INODE_DISK_SLOT_SIZE);
    inode_file.flush();
    return inode_file.good();
}

bool InodeStorage::read_inode(uint64_t ino, Inode& dinode) {
    std::lock_guard<std::mutex> lock(file_mutex);

    std::vector<uint8_t> disk_buffer(INODE_DISK_SLOT_SIZE);
    size_t offset = ino * INODE_DISK_SLOT_SIZE;
    inode_file.seekg(offset);
    inode_file.read(reinterpret_cast<char*>(disk_buffer.data()), INODE_DISK_SLOT_SIZE);
    if (inode_file.gcount() < static_cast<std::streamsize>(INODE_DISK_SLOT_SIZE)) {
        std::cerr << "[READ ERROR] inode file read error at offset(inodeid): "
                  << offset << " ino: " << ino << std::endl;
        return false;
    }

    size_t parse_offset = 0;
    return Inode::deserialize(disk_buffer.data(), parse_offset, dinode, INODE_DISK_SLOT_SIZE);
}

void InodeStorage::expand(size_t new_size) {
    std::lock_guard<std::mutex> lock(file_mutex);
    inode_file.seekp(0, std::ios::end);
    size_t current_size = static_cast<size_t>(inode_file.tellp());
    if (new_size > current_size) {
        inode_file.seekp(new_size - 1);
        inode_file.write("", 1);
        inode_file.flush();
    }
}

size_t InodeStorage::size() {
    inode_file.seekg(0, std::ios::end);
    return static_cast<size_t>(inode_file.tellg());
}

bool InodeStorage::generate_metadata_batch(const InodeStorage::BatchGenerationConfig& config) {
    if (config.output_file.empty()) {
        throw std::invalid_argument("output_file 未设置");
    }

    const size_t batch_size = config.batch_size == 0 ? 1'000'000 : config.batch_size;
    auto nodes = config.node_distribution;
    if (nodes.empty()) {
        nodes.push_back({0, 0, 0.4});   // SSD
        nodes.push_back({200, 1, 0.4}); // HDD
        nodes.push_back({4000, 2, 0.2}); // 混合
    }

    std::vector<double> node_weights;
    node_weights.reserve(nodes.size());
    for (const auto& node : nodes) {
        node_weights.push_back(node.weight > 0.0 ? node.weight : 1.0);
    }
    std::discrete_distribution<size_t> node_picker(node_weights.begin(), node_weights.end());

    std::random_device rd;
    uint32_t seed = config.random_seed == 0 ? rd() : config.random_seed;
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint16_t> block_id_dist(
        0, std::numeric_limits<uint16_t>::max());

    std::ofstream output(config.output_file, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        throw std::runtime_error("无法打开输出文件 " + config.output_file);
    }

    auto now_tp = std::chrono::system_clock::now();

    size_t progress_step = 100'000;
    if (progress_step > batch_size) {
        progress_step = batch_size;
    }

    for (size_t idx = 0; idx < batch_size; ++idx) {
        TemperatureClass temp = pick_temperature(config, rng);
        const auto& node = nodes[node_picker(rng)];
        const InodeStorage::SizeRange& range = range_for_temperature(config, temp);
        uint64_t size_bytes = pick_size(range, rng);

        Inode inode;
        inode.inode = config.starting_inode + idx;
        inode.setNodeId(node.node_id);
        inode.setNodeType(node.node_type);
        inode.setFileType(static_cast<uint8_t>(FileType::Regular));
        inode.setFilePerm(0644);
        inode.setBlockId(block_id_dist(rng));
        std::ostringstream ns_id;
        ns_id << std::setw(Inode::kNamespaceIdLen)
              << std::setfill('0')
              << node.node_id;
        inode.setNamespaceId(ns_id.str());
        inode.setVolumeId(volume_for_temperature(temp));

        inode.setFilename(build_path_name(inode.inode, temp, config, rng));
        inode.setDigest(build_digest(rng));

        uint16_t unit = 0;
        uint16_t value = 0;
        encode_size_fields(size_bytes, unit, value);
        inode.setSizeUnit(unit);
        inode.setFileSize(value);

        size_t block_size_bytes = config.block_size_bytes == 0
            ? 4ULL * 1024 * 1024
            : config.block_size_bytes;
        size_t block_count = static_cast<size_t>((size_bytes + block_size_bytes - 1) / block_size_bytes);
        auto segments = build_segments(block_count, config.max_segments, node, rng);
        inode.clearBlocks();
        inode.appendBlocks(segments);

        apply_temperature_timestamps(inode, temp, rng, now_tp);

        auto serialized = inode.serialize();
        if (serialized.size() > InodeStorage::INODE_DISK_SLOT_SIZE) {
            throw std::runtime_error("序列化 inode 超过 512B 限制");
        }
        std::array<uint8_t, InodeStorage::INODE_DISK_SLOT_SIZE> slot{};
        std::memcpy(slot.data(), serialized.data(), serialized.size());
        output.write(reinterpret_cast<const char*>(slot.data()), slot.size());
        if (!output.good()) {
            throw std::runtime_error("写入失败: " + config.output_file);
        }

        if (config.verbose && ((idx + 1) % progress_step == 0)) {
            std::cout << "[BatchGen] 已生成 " << (idx + 1)
                      << "/" << batch_size << " inodes" << std::endl;
        }
    }

    output.flush();
    if (!output.good()) {
        throw std::runtime_error("输出文件 flush 失败: " + config.output_file);
    }

    if (config.verbose) {
        std::cout << "[BatchGen] 完成批量生成: " << batch_size
                  << " 条 -> " << config.output_file << std::endl;
    }

    return true;
}

// --- BitmapStorage 实现 ---

BitmapStorage::BitmapStorage(const std::string& path, bool create_new) {
    file_path = path;
    if (create_new) {
        std::ofstream reset(file_path, std::ios::binary | std::ios::trunc);
        if (!reset.is_open()) {
            throw std::runtime_error("Failed to create bitmap file");
        }
    }
    // 始终以读写模式打开，便于局部刷新
    std::ios_base::openmode mode = std::ios::in | std::ios::out | std::ios::binary;
    bitmap_file.open(file_path, mode);
    if (!bitmap_file.is_open()) {
        throw std::runtime_error("Failed to open bitmap file");
    }
}

BitmapStorage::~BitmapStorage() {
    if (bitmap_file.is_open()) bitmap_file.close();
}

bool BitmapStorage::write_bitmap(const std::vector<char>& bitmap_data) {
    std::lock_guard<std::mutex> lock(file_mutex);
    bitmap_file.seekp(0);
    bitmap_file.write(bitmap_data.data(), bitmap_data.size());
    bitmap_file.flush();
    return bitmap_file.good();
}

bool BitmapStorage::write_bitmap_region(size_t byte_offset, const char* data, size_t length) {
    std::lock_guard<std::mutex> lock(file_mutex);
    bitmap_file.seekp(static_cast<std::streamoff>(byte_offset));
    bitmap_file.write(data, static_cast<std::streamsize>(length));
    bitmap_file.flush();
    return bitmap_file.good();
}

void BitmapStorage::read_bitmap(std::vector<char>& bitmap_data) {
    std::lock_guard<std::mutex> lock(file_mutex);

    // 移动到文件末尾获取文件大小
    bitmap_file.seekg(0, std::ios::end);
    std::streamsize sz = bitmap_file.tellg();
    if (sz <= 0) {
        bitmap_data.clear();
        return;
    }
    // 移动回文件开头
    bitmap_file.seekg(0);

    // 读数据
    bitmap_data.resize(static_cast<size_t>(sz));
    bitmap_file.read(reinterpret_cast<char*>(bitmap_data.data()), sz);
}