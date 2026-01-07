#include "ClusterManagerServiceImpl.h"

#include <brpc/closure_guard.h>

void ClusterManagerServiceImpl::RegisterNode(::google::protobuf::RpcController*,
                                             const storagenode::RegisterRequest* request,
                                             storagenode::RegisterResponse* response,
                                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!manager_) {
        return;
    }
    manager_->HandleRegister(request, response);
}

void ClusterManagerServiceImpl::Heartbeat(::google::protobuf::RpcController*,
                                          const storagenode::HeartbeatRequest* request,
                                          storagenode::HeartbeatResponse* response,
                                          ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!manager_) {
        return;
    }
    manager_->HandleHeartbeat(request, response);
}
