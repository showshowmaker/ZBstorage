#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "../../../src/mds/inode/inode.h"
#include "../../../src/mds/namespace/Directory.h"
#include "../../../src/fs/volume/VolumeRegistry.h"

// NOTE:
// These lightweight structs play the role of generated proto messages.
// They are intentionally simple so we can wire RPC-like calls without
// adding external protobuf/gRPC dependencies inside tests/.
namespace rpc {

struct Status {
    int code{0};
    std::string message;

    bool ok() const { return code == 0; }
    static Status Ok() { return Status{0, {}}; }
    static Status Error(int code, const std::string& msg) { return Status{code, msg}; }
};

struct VolumeInfo {
    std::shared_ptr<Volume> volume;
    VolumeType type{VolumeType::SSD};
};

struct InodeReply {
    std::shared_ptr<Inode> inode;
};

struct DirectoryList {
    std::vector<DirectoryEntry> entries;
};

struct ColdInodeList {
    std::vector<uint64_t> inodes;
};

struct ColdInodeBitmap {
    std::shared_ptr<boost::dynamic_bitset<>> bitmap;
};

} // namespace rpc
