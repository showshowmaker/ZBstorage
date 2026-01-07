#include "GatewayServiceImpl.h"
#include "common/StatusUtils.h"

void GatewayServiceImpl::Write(::google::protobuf::RpcController* controller,
                               const storagenode::WriteRequest* request,
                               storagenode::WriteReply* response,
                               ::google::protobuf::Closure* done) {
    if (!dispatcher_) {
        if (response) {
            StatusUtils::SetStatus(response->mutable_status(),
                                   rpc::STATUS_UNKNOWN_ERROR,
                                   "Gateway dispatcher not initialized");
        }
        if (done) done->Run();
        return;
    }
    dispatcher_->DispatchWrite(request, response, static_cast<brpc::Controller*>(controller), done);
}

void GatewayServiceImpl::Read(::google::protobuf::RpcController* controller,
                              const storagenode::ReadRequest* request,
                              storagenode::ReadReply* response,
                              ::google::protobuf::Closure* done) {
    if (!dispatcher_) {
        if (response) {
            StatusUtils::SetStatus(response->mutable_status(),
                                   rpc::STATUS_UNKNOWN_ERROR,
                                   "Gateway dispatcher not initialized");
        }
        if (done) done->Run();
        return;
    }
    dispatcher_->DispatchRead(request, response, static_cast<brpc::Controller*>(controller), done);
}

void GatewayServiceImpl::Truncate(::google::protobuf::RpcController* controller,
                                  const storagenode::TruncateRequest* request,
                                  storagenode::TruncateReply* response,
                                  ::google::protobuf::Closure* done) {
    if (!dispatcher_) {
        if (response) {
            StatusUtils::SetStatus(response->mutable_status(),
                                   rpc::STATUS_UNKNOWN_ERROR,
                                   "Gateway dispatcher not initialized");
        }
        if (done) done->Run();
        return;
    }
    dispatcher_->DispatchTruncate(request, response, static_cast<brpc::Controller*>(controller), done);
}
