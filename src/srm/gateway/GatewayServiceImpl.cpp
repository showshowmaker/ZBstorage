#include "GatewayServiceImpl.h"

void GatewayServiceImpl::Write(::google::protobuf::RpcController* controller,
                               const storagenode::WriteRequest* request,
                               storagenode::WriteReply* response,
                               ::google::protobuf::Closure* done) {
    if (!dispatcher_) {
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
        if (done) done->Run();
        return;
    }
    dispatcher_->DispatchRead(request, response, static_cast<brpc::Controller*>(controller), done);
}
