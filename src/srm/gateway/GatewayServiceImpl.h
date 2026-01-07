#pragma once

#include <memory>
#include <cerrno>

#include "storage_node.pb.h"
#include "RequestDispatcher.h"

class GatewayServiceImpl : public storagenode::StorageService {
public:
    explicit GatewayServiceImpl(std::shared_ptr<RequestDispatcher> dispatcher)
        : dispatcher_(std::move(dispatcher)) {}

    void Write(::google::protobuf::RpcController* controller,
               const storagenode::WriteRequest* request,
               storagenode::WriteReply* response,
               ::google::protobuf::Closure* done) override;

    void Read(::google::protobuf::RpcController* controller,
              const storagenode::ReadRequest* request,
              storagenode::ReadReply* response,
              ::google::protobuf::Closure* done) override;

    void Truncate(::google::protobuf::RpcController* controller,
                  const storagenode::TruncateRequest* request,
                  storagenode::TruncateReply* response,
                  ::google::protobuf::Closure* done) override;

    void UnmountDisk(::google::protobuf::RpcController* controller,
                     const storagenode::UnmountRequest* request,
                     storagenode::UnmountReply* response,
                     ::google::protobuf::Closure* done) override {
        // Gateway does not support control commands yet.
        (void)controller;
        (void)request;
        if (response && response->mutable_status()) {
            response->mutable_status()->set_code(ENOTSUP);
            response->mutable_status()->set_message("gateway unmount not supported");
        }
        if (done) done->Run();
    }

private:
    std::shared_ptr<RequestDispatcher> dispatcher_;
};
