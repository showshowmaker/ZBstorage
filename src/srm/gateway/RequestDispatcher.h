#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "storage_node.pb.h"
#include "../StorageNodeManager.h"
#include "../simulation/VirtualNodeEngine.h"

class RequestDispatcher {
public:
    RequestDispatcher(std::shared_ptr<StorageNodeManager> manager,
                      std::shared_ptr<VirtualNodeEngine> virtual_engine)
        : manager_(std::move(manager)), virtual_engine_(std::move(virtual_engine)) {}

    void DispatchWrite(const storagenode::WriteRequest* req,
                       storagenode::WriteReply* resp,
                       brpc::Controller* cntl);

    void DispatchRead(const storagenode::ReadRequest* req,
                      storagenode::ReadReply* resp,
                      brpc::Controller* cntl);

private:
    struct StubEntry {
        std::unique_ptr<brpc::Channel> channel;
        std::unique_ptr<storagenode::StorageService_Stub> stub;
    };

    using StubMap = std::unordered_map<std::string, StubEntry>;

    std::shared_ptr<StorageNodeManager> manager_;
    std::shared_ptr<VirtualNodeEngine> virtual_engine_;
    std::mutex stub_mu_;
    StubMap stubs_;

    storagenode::StorageService_Stub* GetStub(const NodeContext& ctx);
    void FillStatus(rpc::Status* status, int code, const std::string& msg);
};
