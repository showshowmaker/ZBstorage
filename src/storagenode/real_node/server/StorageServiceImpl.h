#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "storage_node.pb.h"
#include "common/StatusUtils.h"
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

    void Truncate(::google::protobuf::RpcController* controller,
                  const storagenode::TruncateRequest* request,
                  storagenode::TruncateReply* response,
                  ::google::protobuf::Closure* done) override;

    void UnmountDisk(::google::protobuf::RpcController* controller,
                     const storagenode::UnmountRequest* request,
                     storagenode::UnmountReply* response,
                     ::google::protobuf::Closure* done) override;

private:
    uint64_t ComputeChecksum(const void* data, size_t len) const;

    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<LocalMetadataManager> metadata_mgr_;
    std::shared_ptr<IOEngine> io_engine_;
    bool ready_{false};
};
