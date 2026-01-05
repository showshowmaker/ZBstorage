#pragma once

#include <memory>
#include <string>

#include <brpc/channel.h>

#include "mds.pb.h"
#include "vfs.pb.h"
#include "storage_node.pb.h"
#include "MountConfig.h"

struct RpcBundle {
    std::unique_ptr<brpc::Channel> channel;
};

class RpcClients {
public:
    explicit RpcClients(MountConfig cfg) : cfg_(std::move(cfg)) {}

    bool Init();

    rpc::MdsService_Stub* mds() { return mds_stub_.get(); }
    rpc::VfsService_Stub* vfs() { return vfs_stub_.get(); }
    storagenode::StorageService_Stub* srm() { return srm_stub_.get(); }

private:
    MountConfig cfg_;
    std::unique_ptr<brpc::Channel> mds_channel_;
    std::unique_ptr<brpc::Channel> vfs_channel_;
    std::unique_ptr<brpc::Channel> srm_channel_;
    std::unique_ptr<rpc::MdsService_Stub> mds_stub_;
    std::unique_ptr<rpc::VfsService_Stub> vfs_stub_;
    std::unique_ptr<storagenode::StorageService_Stub> srm_stub_;
};
