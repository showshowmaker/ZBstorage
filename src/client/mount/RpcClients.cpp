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
    srm_channel_ = std::make_unique<brpc::Channel>();
    if (srm_channel_->Init(cfg_.srm_addr.c_str(), &opts) != 0) {
        srm_channel_.reset();
        return false;
    }

    mds_stub_ = std::make_unique<rpc::MdsService_Stub>(mds_channel_.get());
    srm_stub_ = std::make_unique<storagenode::StorageService_Stub>(srm_channel_.get());
    return true;
}
