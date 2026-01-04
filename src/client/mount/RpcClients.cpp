#include "RpcClients.h"

bool RpcClients::Init() {
    brpc::ChannelOptions opts;
    opts.protocol = "baidu_std";
    opts.timeout_ms = cfg_.rpc_timeout_ms;
    opts.max_retry = cfg_.rpc_max_retry;

    mds_channel_ = std::make_unique<brpc::Channel>();
    if (mds_channel_->Init(cfg_.mds_addr.c_str(), &opts) != 0) {
        mds_channel_.reset();
        return false;
    }
    vfs_channel_ = std::make_unique<brpc::Channel>();
    if (vfs_channel_->Init(cfg_.vfs_addr.c_str(), &opts) != 0) {
        vfs_channel_.reset();
        return false;
    }

    mds_stub_ = std::make_unique<rpc::MdsService_Stub>(mds_channel_.get());
    vfs_stub_ = std::make_unique<rpc::VfsService_Stub>(vfs_channel_.get());
    return true;
}
