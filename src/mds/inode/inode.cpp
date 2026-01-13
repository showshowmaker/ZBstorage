#include "inode.h"

Inode::Inode()
    : location_id{.raw=0},
        block_id(0),
        filename_len(0),
        digest_len(0),
        file_mode{.raw=0},
        file_size{.raw=0},
        inode(0),
        namespace_id(std::string(kNamespaceIdLen, '0')) {}

void Inode::setNodeId(uint16_t id) {
    location_id.fields.node_id = id & 0x3FFF;
    im_time = InodeTimestamp();
}
void Inode::setNodeType(uint8_t type) {
    location_id.fields.node_type = type & 0x03;
    im_time = InodeTimestamp();
}
void Inode::setBlockId(uint16_t id) {
    block_id = id;
    im_time = InodeTimestamp();
}

namespace {
std::string NormalizeNamespaceId(const std::string& id) {
    if (id.size() == Inode::kNamespaceIdLen) {
        return id;
    }
    if (id.size() > Inode::kNamespaceIdLen) {
        return id.substr(id.size() - Inode::kNamespaceIdLen);
    }
    return std::string(Inode::kNamespaceIdLen - id.size(), '0') + id;
}
}

void Inode::setNamespaceId(const std::string& id) {
    namespace_id = NormalizeNamespaceId(id);
    im_time = InodeTimestamp();
}

const std::string& Inode::getNamespaceId() const {
    return namespace_id;
}

void Inode::setFilename(const std::string& name) {
    filename = name;
    filename_len = name.size();
    im_time = InodeTimestamp();
}
void Inode::setDigest(const std::vector<uint8_t>& dig) {
    digest = dig;
    digest_len = dig.size();
    im_time = InodeTimestamp();
}
void Inode::setFileType(uint8_t type) {
    file_mode.fields.file_type = type;
    im_time = InodeTimestamp();
}
//changed
void Inode::setFilePerm(uint16_t perm) {
    file_mode.fields.file_perm = perm&0x0FFF;
    im_time = InodeTimestamp();
}
void Inode::setSizeUnit(uint16_t unit) {
    file_size.fields.size_unit = unit;
    im_time = InodeTimestamp();
}
void Inode::setFileSize(uint16_t size) {
    file_size.fields.file_size = size;
    im_time = InodeTimestamp();
}
void Inode::setFmTime(const InodeTimestamp& t) {
    fm_time = t;
    im_time = InodeTimestamp();
}
void Inode::setFaTime(const InodeTimestamp& t) {
    fa_time = t;
    im_time = InodeTimestamp();
}
void Inode::setFcTime(const InodeTimestamp& t) {
    fc_time = t;
    im_time = InodeTimestamp();
}
void Inode::setVolumeId(const std::string& id)
{
    volume_id = id;
    im_time = InodeTimestamp(); // 更新修改时间
}

const std::string& Inode::getVolumeUUID() const {
    return volume_id;
}

uint64_t Inode::getFileSize() const {
    uint64_t value = file_size.fields.file_size;
    switch (file_size.fields.size_unit) {
    case 0: return value;                                 // bytes
    case 1: return value * 1024ULL;                       // KB
    case 2: return value * 1024ULL * 1024ULL;             // MB
    case 3: return value * 1024ULL * 1024ULL * 1024ULL;   // GB
    default: return value;
    }
}

const std::vector<BlockSegment>& Inode::getBlocks() const {
    return block_segments;
}

void Inode::appendBlocks(const std::vector<BlockSegment>& segments) {
    if (segments.empty()) return;
    block_segments.insert(block_segments.end(), segments.begin(), segments.end());
    im_time = InodeTimestamp();
}

void Inode::clearBlocks() {
    block_segments.clear();
    im_time = InodeTimestamp();
}

size_t Inode::block_count() const { 
        size_t cnt = 0;
        for (const auto& seg : block_segments) cnt += seg.block_count;
        return cnt;
    }

bool Inode::find_physical_block(size_t logical_block, size_t& physical_block) const {
    for (const auto& seg : block_segments) {
        if (logical_block >= seg.logical_start && logical_block < seg.logical_start + seg.block_count) {
            physical_block = seg.start_block + (logical_block - seg.logical_start);
            return true;
        }
    }
    return false;
}

// 将inode对象转成一串字节数组，用于写入文件
std::vector<uint8_t> Inode::serialize() const {
    std::vector<uint8_t> buf;
    // 
    buf.reserve(256);

    // 固定字段
    auto append = [&](const void* ptr, size_t len) {
        const uint8_t* p = reinterpret_cast<const uint8_t*>(ptr);
        buf.insert(buf.end(), p, p + len);
    };

    append(&location_id.raw, sizeof(location_id.raw));
    append(&block_id, sizeof(block_id));
    append(&filename_len, sizeof(filename_len));
    append(&digest_len, sizeof(digest_len));
    append(&file_mode.raw, sizeof(file_mode.raw));
    append(&file_size.raw, sizeof(file_size.raw));
    append(&inode, sizeof(inode));
    const std::string ns = NormalizeNamespaceId(namespace_id);
    append(ns.data(), kNamespaceIdLen);
    append(&fm_time, sizeof(fm_time));
    append(&fa_time, sizeof(fa_time));
    append(&im_time, sizeof(im_time));
    append(&fc_time, sizeof(fc_time));

    // 可变字段
    append(filename.data(), filename.size());
    append(digest.data(), digest.size());

    uint8_t volume_id_len = volume_id.size();
    append(&volume_id_len, sizeof(volume_id_len));
    append(volume_id.data(),volume_id.size());

    // block_segments
    uint32_t segment_count = block_segments.size();
    append(&segment_count, sizeof(segment_count));
    for (const auto& seg : block_segments) {
        append(&seg, sizeof(seg));
    }

    return buf;
}

bool Inode::deserialize(const uint8_t* data, size_t& offset, Inode& out, size_t total_size) {
    // 防止越界
    auto safe_read = [&](void* dest, size_t len) ->bool {
        if(offset+len>total_size) return false;
        std::memcpy(dest, data + offset, len);
        offset += len;
        return true;
    };

    if(!safe_read(&out.location_id.raw, sizeof(out.location_id.raw))) return false;
    if(!safe_read(&out.block_id, sizeof(out.block_id))) return false;
    if(!safe_read(&out.filename_len, sizeof(out.filename_len))) return false;
    if(!safe_read(&out.digest_len, sizeof(out.digest_len))) return false;
    if(!safe_read(&out.file_mode.raw, sizeof(out.file_mode.raw))) return false;
    if(!safe_read(&out.file_size.raw, sizeof(out.file_size.raw))) return false;
    if(!safe_read(&out.inode, sizeof(out.inode))) return false;
    if(offset + Inode::kNamespaceIdLen > total_size) return false;
    out.namespace_id.assign(reinterpret_cast<const char*>(data + offset), Inode::kNamespaceIdLen);
    offset += Inode::kNamespaceIdLen;
    if(!safe_read(&out.fm_time, sizeof(out.fm_time))) return false;
    if(!safe_read(&out.fa_time, sizeof(out.fa_time))) return false;
    if(!safe_read(&out.im_time, sizeof(out.im_time))) return false;
    if(!safe_read(&out.fc_time, sizeof(out.fc_time))) return false;

    // 变长字段检查
    if(offset+out.filename_len+out.digest_len>total_size) return false;
    out.filename.assign(reinterpret_cast<const char*>(data + offset), out.filename_len);
    offset += out.filename_len;

    out.digest.resize(out.digest_len);
    std::memcpy(out.digest.data(), data + offset, out.digest_len);
    offset += out.digest_len;

    uint8_t volume_id_len = 0;
    if (!safe_read(&volume_id_len, sizeof(volume_id_len))) return false;
    out.volume_id.assign(reinterpret_cast<const char*>(data + offset), volume_id_len);
    offset += volume_id_len;

    uint32_t segment_count = 0;
    if(!safe_read(&segment_count, sizeof(segment_count))) return false;
    out.block_segments.resize(segment_count);
    for (uint32_t i = 0; i < segment_count; ++i) {
        if(!safe_read(&out.block_segments[i], sizeof(BlockSegment))) return false;
    }

    return true;
}