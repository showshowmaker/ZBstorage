// src/server/storage_main.cpp
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <brpc/server.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include "../proto/common.pb.h"
#include "../proto/mds.pb.h"
#include "../proto/storage.pb.h"

#include "fs/volume/Volume.h"
#include "fs/volume/VolumeManager.h"
#include "srm/storage_manager/StorageResource.h"

using fs::common::StatusCode;

namespace {

// 简单命令行解析：--port=8002 --mds_addr=127.0.0.1:8001
struct StorageFlags {
    int port = 8002;
    std::string mds_addr = "127.0.0.1:8001";
};

StorageFlags ParseFlags(int argc, char* argv[]) {
    StorageFlags f;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--port=", 0) == 0) {
            f.port = std::stoi(arg.substr(std::string("--port=").size()));
        } else if (arg.rfind("--mds_addr=", 0) == 0) {
            f.mds_addr = arg.substr(std::string("--mds_addr=").size());
        }
    }
    return f;
}

} // namespace

class StorageServiceImpl : public fs::storage::StorageService {
public:
    explicit StorageServiceImpl(std::shared_ptr<VolumeManager> vm)
        : vm_(std::move(vm)) {}

    void Write(google::protobuf::RpcController* cntl_base,
               const fs::storage::WriteRequest* req,
               fs::storage::WriteResponse* res,
               google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        auto vol = vm_->get_volume(req->volume_id());  // 假定你在 VolumeManager 里实现这个接口
        if (!vol) {
            res->set_status(StatusCode::NOT_FOUND);
            return;
        }

        ssize_t n = vol->write(req->offset(),
                               req->data().data(),
                               req->data().size());     // 假定 Volume::write(offset, buf, len)

        if (n < 0) {
            res->set_status(StatusCode::IO_ERROR);
            return;
        }

        res->set_status(StatusCode::OK);
        res->set_bytes_written(static_cast<int32_t>(n));
    }

    void Read(google::protobuf::RpcController* cntl_base,
              const fs::storage::ReadRequest* req,
              fs::storage::ReadResponse* res,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);

        auto vol = vm_->get_volume(req->volume_id());
        if (!vol) {
            res->set_status(StatusCode::NOT_FOUND);
            return;
        }

        std::string buffer;
        buffer.resize(req->length());
        ssize_t n = vol->read(req->offset(), &buffer[0], buffer.size()); // 假定 Volume::read

        if (n < 0) {
            res->set_status(StatusCode::IO_ERROR);
            return;
        }

        buffer.resize(static_cast<size_t>(n));
        res->set_status(StatusCode::OK);
        res->set_data(buffer);
    }

private:
    std::shared_ptr<VolumeManager> vm_;
};

int main(int argc, char* argv[]) {
    StorageFlags flags = ParseFlags(argc, argv);

    // 1. 初始化 VolumeManager，加载本地卷
    auto volume_mgr = std::make_shared<VolumeManager>();

    StorageResource storage_resource;
    storage_resource.loadFromFile(false, false);  // 与 test_vfs_new 中一致:contentReference[oaicite:2]{index=2}

    std::vector<uint64_t> local_volume_ids;
    uint64_t next_vol_id = 1;

    while (true) {
        auto node_volumes = storage_resource.initOneNodeVolume();
        if (!node_volumes.first && !node_volumes.second) {
            break;
        }
        if (node_volumes.first) {
            uint64_t id = next_vol_id++;
            volume_mgr->add_volume(id, node_volumes.first);  // 需要在 VolumeManager 中实现 add_volume(id, vol)
            local_volume_ids.push_back(id);
        }
        if (node_volumes.second) {
            uint64_t id = next_vol_id++;
            volume_mgr->add_volume(id, node_volumes.second);
            local_volume_ids.push_back(id);
        }
    }

    if (local_volume_ids.empty()) {
        std::cout << "[storage] no volumes from StorageResource, using fallback volume\n";
        // 这里的 Volume 构造和参数来自你原来的测试代码:contentReference[oaicite:3]{index=3}
        auto fallback_vol = std::make_shared<Volume>("vol-1", "node-1", 4096);
        uint64_t id = next_vol_id++;
        volume_mgr->add_volume(id, fallback_vol);
        local_volume_ids.push_back(id);
    }

    // 2. 启动 Storage RPC Server
    brpc::Server server;
    StorageServiceImpl service(volume_mgr);

    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "[storage] AddService failed\n";
        return -1;
    }
    if (server.Start(flags.port, nullptr) != 0) {
        std::cerr << "[storage] Start server on port " << flags.port << " failed\n";
        return -1;
    }

    // 3. 向 MDS 注册自身
    brpc::Channel mds_channel;
    brpc::ChannelOptions options;
    if (mds_channel.Init(flags.mds_addr.c_str(), &options) != 0) {
        std::cerr << "[storage] init mds channel " << flags.mds_addr << " failed\n";
        return -1;
    }

    fs::mds::MdsService_Stub stub(&mds_channel);
    fs::mds::RegisterNodeRequest req;
    fs::mds::RegisterNodeResponse resp;
    brpc::Controller cntl;

    // 假设本机地址为 127.0.0.1
    req.set_address("127.0.0.1:" + std::to_string(flags.port));
    for (auto vid : local_volume_ids) {
        req.add_volume_ids(std::to_string(vid));  // 用字符串上报
    }

    stub.RegisterNode(&cntl, &req, &resp, nullptr);
    if (cntl.Failed() || resp.status() != StatusCode::OK) {
        std::cerr << "[storage] register to mds failed, error=" << cntl.ErrorText() << "\n";
        return -1;
    }

    std::cout << "[storage] started at port " << flags.port
              << ", registered " << local_volume_ids.size()
              << " volumes to mds " << flags.mds_addr << "\n";

    server.RunUntilAskedToQuit();
    return 0;
}
