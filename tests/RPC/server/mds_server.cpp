// src/server/mds_main.cpp
#include <atomic>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <brpc/server.h>
#include <brpc/controller.h>

#include "proto/common.pb.h"
#include "proto/mds.pb.h"

#include "fs/mds/MdsServer.h"   // 根据你的实际路径调整
// 如果你有 MetadataStore 等，可以再按需 include

using fs::common::StatusCode;

namespace {

struct MdsFlags {
    int port = 8001;
    std::string dir = "/tmp/vfs_mds";
};

MdsFlags ParseFlags(int argc, char* argv[]) {
    MdsFlags f;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--port=", 0) == 0) {
            f.port = std::stoi(arg.substr(std::string("--port=").size()));
        } else if (arg.rfind("--dir=", 0) == 0) {
            f.dir = arg.substr(std::string("--dir=").size());
        }
    }
    return f;
}

} // namespace

class MdsServiceImpl : public fs::mds::MdsService {
public:
    explicit MdsServiceImpl(std::shared_ptr<MdsServer> mds)
        : mds_(std::move(mds)) {}

    // 1. Storage 节点注册：只维护简单路由信息
    void RegisterNode(google::protobuf::RpcController* cntl_base,
                      const fs::mds::RegisterNodeRequest* req,
                      fs::mds::RegisterNodeResponse* res,
                      google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        if (req->volume_ids_size() == 0) {
            res->set_status(StatusCode::IO_ERROR);
            return;
        }

        for (const auto& vid_str : req->volume_ids()) {
            VolumeRoute route;
            route.storage_addr = req->address();
            route.volume_id = std::stoull(vid_str);
            routes_.push_back(route);
        }

        res->set_status(StatusCode::OK);
        std::cout << "[mds] register node " << req->address()
                  << " with " << req->volume_ids_size() << " volumes\n";
    }

    // 2. 创建文件：包装 MdsServer 的逻辑
    void CreateFile(google::protobuf::RpcController* cntl_base,
                    const fs::mds::CreateFileRequest* req,
                    fs::mds::CreateFileResponse* res,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        // 假定 MdsServer::create_file(path, mode) -> inode_id (0 代表失败)
        uint64_t ino = mds_->create_file(req->path(), req->mode());
        if (ino == 0) {
            res->set_status(StatusCode::IO_ERROR);
            return;
        }
        res->set_status(StatusCode::OK);
        res->set_inode_id(ino);
    }

    // 3. Lookup：路径 -> inode_id
    void Lookup(google::protobuf::RpcController* cntl_base,
                const fs::mds::LookupRequest* req,
                fs::mds::LookupResponse* res,
                google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        // 假定 MdsServer::lookup_inode(path) -> inode_id; -1 或 0 表示不存在
        uint64_t ino = mds_->lookup_inode(req->path());
        if (ino == static_cast<uint64_t>(-1) || ino == 0) {
            res->set_status(StatusCode::NOT_FOUND);
            return;
        }
        res->set_status(StatusCode::OK);
        res->set_inode_id(ino);
    }

    // 4. GetBlockInfo：简化版本的调度策略（轮询在已注册卷里选一个）
    void GetBlockInfo(google::protobuf::RpcController* cntl_base,
                      const fs::mds::GetBlockInfoRequest* req,
                      fs::mds::GetBlockInfoResponse* res,
                      google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        if (routes_.empty()) {
            res->set_status(StatusCode::NOT_FOUND);
            return;
        }

        // 简单轮询：实际可以做真正的 layout 查询
        size_t idx = rr_index_.fetch_add(1, std::memory_order_relaxed);
        const auto& route = routes_[idx % routes_.size()];

        res->set_status(StatusCode::OK);
        res->set_storage_addr(route.storage_addr);
        res->set_volume_id(route.volume_id);
        res->set_block_offset(req->file_offset());  // 简单映射：文件 offset = 卷 offset
        res->set_block_length(4096);                // 固定块大小，后续可改成真实值
    }

private:
    struct VolumeRoute {
        std::string storage_addr;
        uint64_t volume_id;
    };

    std::shared_ptr<MdsServer> mds_;
    std::vector<VolumeRoute> routes_;
    std::atomic<size_t> rr_index_{0};
};

int main(int argc, char* argv[]) {
    MdsFlags flags = ParseFlags(argc, argv);

    std::filesystem::create_directories(flags.dir);
    const std::string inode_file   = flags.dir + "/inode.dat";
    const std::string bitmap_file  = flags.dir + "/bitmap.dat";
    const std::string dir_store    = flags.dir + "/dir_store";

    std::filesystem::create_directories(dir_store);

    // 按照你现在 test_vfs_new.cpp 的初始化方式，把逻辑搬到这里:contentReference[oaicite:5]{index=5}
    auto mds = std::make_shared<MdsServer>(inode_file, bitmap_file, dir_store, true);

    // 如果 MdsServer 提供 startup/create_root_directory 等接口，可以在这里做幂等初始化
    if (!mds->startup()) {
        std::cerr << "[mds] startup() failed\n";
        return -1;
    }
    if (!mds->create_root_directory()) {
        std::cerr << "[mds] create_root_directory() failed\n";
        // 不一定要退出，看你实现
    }

    brpc::Server server;
    MdsServiceImpl service(mds);

    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "[mds] AddService failed\n";
        return -1;
    }
    if (server.Start(flags.port, nullptr) != 0) {
        std::cerr << "[mds] Start server on port " << flags.port << " failed\n";
        return -1;
    }

    std::cout << "[mds] started at port " << flags.port
              << ", meta dir " << flags.dir << "\n";

    server.RunUntilAskedToQuit();
    return 0;
}
