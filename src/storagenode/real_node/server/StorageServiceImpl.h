#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "storage_node.pb.h"
#include "../io/DiskManager.h"
#include "../io/IOEngine.h"
#include "../meta/LocalMetadataManager.h"

class StorageServiceImpl : public storagenode::StorageService {
public:
    StorageServiceImpl(std::shared_ptr<DiskManager> disk_manager,
                       std::shared_ptr<LocalMetadataManager> metadata_mgr,
                       std::shared_ptr<IOEngine> io_engine);

    void Write(::google::protobuf::RpcController* controller,
               const storagenode::WriteRequest* request,
               storagenode::WriteReply* response,
               ::google::protobuf::Closure* done) override;

    void Read(::google::protobuf::RpcController* controller,
              const storagenode::ReadRequest* request,
              storagenode::ReadReply* response,
              ::google::protobuf::Closure* done) override;

private:
    uint64_t ComputeChecksum(const void* data, size_t len) const;
    void FillStatus(rpc::Status* status, int err, const std::string& message) const;

    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<LocalMetadataManager> metadata_mgr_;
    std::shared_ptr<IOEngine> io_engine_;
    bool ready_{false};
};
