#include "mds_server.h"
#include "../../../src/fs/volume/VolumeRegistry.h"

namespace rpc {

RpcMdsServer::RpcMdsServer(const std::string& inode_path,
                           const std::string& bitmap_path,
                           const std::string& dir_store_base,
                           bool create_new,
                           const std::string& registry_base)
    : mds_(std::make_shared<MdsServer>(inode_path, bitmap_path, dir_store_base, create_new)) {
    if (mds_) {
        try {
            mds_->set_volume_registry(make_file_volume_registry(registry_base));
        } catch (...) {
            // keep running without registry if creation fails
        }
    }
}

Status RpcMdsServer::CreateRoot() {
    return mds_ && mds_->CreateRoot() ? Status::Ok()
                                      : Status::Error(1, "CreateRoot failed");
}

Status RpcMdsServer::Mkdir(const std::string& path, mode_t mode) {
    return mds_ && mds_->Mkdir(path, mode) ? Status::Ok()
                                           : Status::Error(1, "Mkdir failed");
}

Status RpcMdsServer::Rmdir(const std::string& path) {
    return mds_ && mds_->Rmdir(path) ? Status::Ok()
                                     : Status::Error(1, "Rmdir failed");
}

Status RpcMdsServer::CreateFile(const std::string& path, mode_t mode) {
    return mds_ && mds_->CreateFile(path, mode) ? Status::Ok()
                                                : Status::Error(1, "CreateFile failed");
}

RemoveFileReply RpcMdsServer::RemoveFile(const std::string& path) {
    RemoveFileReply reply;
    uint64_t ino = mds_ ? mds_->LookupIno(path) : static_cast<uint64_t>(-1);
    bool ok = mds_ && mds_->RemoveFile(path);
    if (ok && ino != static_cast<uint64_t>(-1)) {
        reply.detached_inodes.push_back(ino);
    }
    reply.status = ok ? Status::Ok() : Status::Error(1, "RemoveFile failed");
    return reply;
}

Status RpcMdsServer::TruncateFile(const std::string& path) {
    return mds_ && mds_->TruncateFile(path) ? Status::Ok()
                                            : Status::Error(1, "TruncateFile failed");
}

Status RpcMdsServer::Ls(const std::string& path, DirectoryList& out) {
    if (!mds_) return Status::Error(1, "MDS not initialized");
    auto inode = mds_->FindInodeByPath(path);
    if (!inode || inode->getFileType() != FileType::Directory) {
        return Status::Error(2, "Path is not directory");
    }
    out.entries = mds_->ReadDirectoryEntries(inode);
    return Status::Ok();
}

uint64_t RpcMdsServer::LookupIno(const std::string& path) {
    return mds_ ? mds_->LookupIno(path) : static_cast<uint64_t>(-1);
}

std::shared_ptr<Inode> RpcMdsServer::FindInode(const std::string& path) {
    return mds_ ? mds_->FindInodeByPath(path) : nullptr;
}

Status RpcMdsServer::WriteInode(uint64_t ino, const Inode& inode) {
    return mds_ && mds_->WriteInode(ino, inode) ? Status::Ok()
                                                : Status::Error(1, "WriteInode failed");
}

uint64_t RpcMdsServer::GetTotalInodes() const {
    return mds_ ? mds_->GetTotalInodes() : 0;
}

uint64_t RpcMdsServer::GetRootInode() const {
    return mds_ ? mds_->GetRootInode() : static_cast<uint64_t>(-1);
}

ColdInodeList RpcMdsServer::CollectColdInodes(size_t max_candidates, size_t min_age_windows) {
    ColdInodeList list;
    if (mds_) {
        list.inodes = mds_->CollectColdInodes(max_candidates, min_age_windows);
    }
    return list;
}

ColdInodeBitmap RpcMdsServer::CollectColdInodesBitmap(size_t min_age_windows) {
    ColdInodeBitmap result;
    if (mds_) {
        result.bitmap = mds_->CollectColdInodesBitmap(min_age_windows);
    }
    return result;
}

ColdInodeList RpcMdsServer::CollectColdInodesByPercent(double percent) {
    ColdInodeList list;
    if (mds_) {
        list.inodes = mds_->CollectColdInodesByAtimePercent(percent);
    }
    return list;
}

Status RpcMdsServer::RegisterVolume(const std::shared_ptr<Volume>& vol,
                                    VolumeType type,
                                    int* out_index,
                                    bool persist_now) {
    return mds_ && mds_->RegisterVolume(vol, type, out_index, persist_now)
               ? Status::Ok()
               : Status::Error(1, "RegisterVolume failed");
}

Status RpcMdsServer::RebuildInodeTable() {
    if (!mds_) return Status::Error(1, "MDS not initialized");
    mds_->RebuildInodeTable();
    return Status::Ok();
}

// -------- Client thin wrappers --------

Status RpcMdsClient::CreateRoot() { return server_->CreateRoot(); }
Status RpcMdsClient::Mkdir(const std::string& path, mode_t mode) { return server_->Mkdir(path, mode); }
Status RpcMdsClient::Rmdir(const std::string& path) { return server_->Rmdir(path); }
Status RpcMdsClient::CreateFile(const std::string& path, mode_t mode) { return server_->CreateFile(path, mode); }
RemoveFileReply RpcMdsClient::RemoveFile(const std::string& path) { return server_->RemoveFile(path); }
Status RpcMdsClient::TruncateFile(const std::string& path) { return server_->TruncateFile(path); }
Status RpcMdsClient::Ls(const std::string& path, DirectoryList& out) { return server_->Ls(path, out); }
uint64_t RpcMdsClient::LookupIno(const std::string& path) { return server_->LookupIno(path); }
std::shared_ptr<Inode> RpcMdsClient::FindInode(const std::string& path) { return server_->FindInode(path); }
Status RpcMdsClient::WriteInode(uint64_t ino, const Inode& inode) { return server_->WriteInode(ino, inode); }
uint64_t RpcMdsClient::GetTotalInodes() const { return server_->GetTotalInodes(); }
uint64_t RpcMdsClient::GetRootInode() const { return server_->GetRootInode(); }
ColdInodeList RpcMdsClient::CollectColdInodes(size_t max_candidates, size_t min_age_windows) { return server_->CollectColdInodes(max_candidates, min_age_windows); }
ColdInodeBitmap RpcMdsClient::CollectColdInodesBitmap(size_t min_age_windows) { return server_->CollectColdInodesBitmap(min_age_windows); }
ColdInodeList RpcMdsClient::CollectColdInodesByPercent(double percent) { return server_->CollectColdInodesByPercent(percent); }
Status RpcMdsClient::RegisterVolume(const std::shared_ptr<Volume>& vol,
                                    VolumeType type,
                                    int* out_index,
                                    bool persist_now) {
    return server_->RegisterVolume(vol, type, out_index, persist_now);
}
Status RpcMdsClient::RebuildInodeTable() { return server_->RebuildInodeTable(); }

} // namespace rpc

#ifndef RPC_BUILD_LIBRARY_ONLY
int main() {
    // Standalone build target placeholder to satisfy tests/CMakeLists globbed executables.
    return 0;
}
#endif
