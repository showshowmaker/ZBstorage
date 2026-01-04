#include "GatewayServiceImpl.h"

#include <brpc/closure_guard.h>

void GatewayServiceImpl::Write(::google::protobuf::RpcController* controller,
                               const storagenode::WriteRequest* request,
                               storagenode::WriteReply* response,
                               ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!dispatcher_) {
        return;
    }
    dispatcher_->DispatchWrite(request, response, static_cast<brpc::Controller*>(controller));
}

void GatewayServiceImpl::Read(::google::protobuf::RpcController* controller,
                              const storagenode::ReadRequest* request,
                              storagenode::ReadReply* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!dispatcher_) {
        return;
    }
    dispatcher_->DispatchRead(request, response, static_cast<brpc::Controller*>(controller));
}
