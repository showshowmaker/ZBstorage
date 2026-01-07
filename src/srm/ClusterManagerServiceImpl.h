#pragma once

#include <memory>

#include "cluster_manager.pb.h"
#include "StorageNodeManager.h"

class ClusterManagerServiceImpl : public storagenode::ClusterManagerService {
public:
    explicit ClusterManagerServiceImpl(std::shared_ptr<StorageNodeManager> manager)
        : manager_(std::move(manager)) {}

    void RegisterNode(::google::protobuf::RpcController* controller,
                      const storagenode::RegisterRequest* request,
                      storagenode::RegisterResponse* response,
                      ::google::protobuf::Closure* done) override;

    void Heartbeat(::google::protobuf::RpcController* controller,
                   const storagenode::HeartbeatRequest* request,
                   storagenode::HeartbeatResponse* response,
                   ::google::protobuf::Closure* done) override;

private:
    std::shared_ptr<StorageNodeManager> manager_;
};
