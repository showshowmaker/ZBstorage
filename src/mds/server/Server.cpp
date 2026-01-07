#include "Server.h"
#include <algorithm>
#include <iostream>
#include <cmath>
#include <cstring>
#include <limits>
#include <mutex>
#include <shared_mutex>

// 移除对 Volume/BlockStore 的依赖
// #include "../../fs/volume/Volume.h"

using mds::DirectoryLockGuard;
using mds::DirectoryLockMode;
using mds::DirectoryLockTable;

namespace {

struct DirectoryLockRequest {
    uint64_t inode{ static_cast<uint64_t>(-1) };
    DirectoryLockMode mode{ DirectoryLockMode::kShared };
};

std::vector<DirectoryLockGuard> acquire_directory_locks(
    DirectoryLockTable& table,
    std::vector<DirectoryLockRequest> requests) {
    const uint64_t invalid = static_cast<uint64_t>(-1);
    requests.erase(std::remove_if(requests.begin(), requests.end(),
        [invalid](const DirectoryLockRequest& req) {
            return req.inode == invalid;
        }), requests.end());
    if (requests.empty()) return {};
    std::sort(requests.begin(), requests.end(),
        [](const DirectoryLockRequest& lhs, const DirectoryLockRequest& rhs) {
            if (lhs.inode != rhs.inode) return lhs.inode < rhs.inode;
            return static_cast<int>(lhs.mode) < static_cast<int>(rhs.mode);
        });
    std::vector<DirectoryLockRequest> deduped;
    deduped.reserve(requests.size());
    for (const auto& req : requests) {
        if (!deduped.empty() && deduped.back().inode == req.inode) {
            if (req.mode == DirectoryLockMode::kExclusive) {
                deduped.back().mode = DirectoryLockMode::kExclusive;
            }
        } else {
            deduped.push_back(req);
        }
    }
    std::vector<DirectoryLockGuard> guards;
    guards.reserve(deduped.size());
    for (const auto& req : deduped) {
        guards.emplace_back(table, req.inode, req.mode);
    }
    return guards;
}

} // namespace

static uint32_t inode_timestamp_key(const InodeTimestamp& t) {
    uint32_t key = 0;
    key |= (static_cast<uint32_t>(t.year) & 0xFF) << 24;
    key |= (static_cast<uint32_t>(t.month) & 0x3F) << 18;
    key |= (static_cast<uint32_t>(t.day) & 0x3F) << 12;
    key |= (static_cast<uint32_t>(t.hour) & 0x3F) << 6;
    key |= (static_cast<uint32_t>(t.minute) & 0x3F);
    return key;
}

MdsServer::MdsServer(bool create_new)
    : meta_(std::make_unique<MetadataManager>(INODE_STORAGE_PATH, INODE_BITMAP_PATH, create_new)),
      dir_store_(std::make_unique<DirStore>("./mds_meta")),
      dir_lock_table_()
{
    // 可选：CreateRoot() 或 RebuildInodeTable()
}

void MdsServer::set_volume_registry(std::shared_ptr<IVolumeRegistry> registry) {
    volume_registry_ = std::move(registry);
    if (volume_registry_) {
        volume_allocator_ = std::make_unique<VolumeAllocator>(volume_registry_);
    } else {
        volume_allocator_.reset();
    }
}

std::shared_ptr<IVolumeRegistry> MdsServer::volume_registry() const {
    return volume_registry_;
}

void MdsServer::set_volume_manager(std::shared_ptr<VolumeManager> manager) {
    volume_manager_ = std::move(manager);
}

void MdsServer::set_handle_observer(std::weak_ptr<IHandleObserver> observer) {
    handle_observer_ = std::move(observer);
}

bool MdsServer::RegisterVolume(const std::shared_ptr<Volume>& vol,
                               VolumeType type,
                               int* out_index,
                               bool persist_now) {
    if (!volume_registry_ || !vol) {
        return false;
    }
    return volume_registry_->register_volume(vol, type, out_index, persist_now);
}

// 新增实现：与测试用例签名一致
MdsServer::MdsServer(const std::string& inode_path,
                     const std::string& bitmap_path,
                     const std::string& dir_store_base,
                     bool create_new)
    : meta_(std::make_unique<MetadataManager>(inode_path, bitmap_path, create_new)),
    dir_store_(std::make_unique<DirStore>(dir_store_base)),
    dir_lock_table_()
{
    // 可选：CreateRoot() 或 RebuildInodeTable()
}

// ========== 命名空间/路径（保持与原逻辑一致） ==========

bool MdsServer::CreateRoot() {
    const std::string root_path = "/";
    {
        std::shared_lock<std::shared_mutex> lk(mtx_namespace_);
        if (inode_table_.find(root_path) != inode_table_.end()) return true;
    }

    auto inode = std::make_shared<Inode>();
    inode->setFilename(root_path);
    inode->setFileType(static_cast<uint8_t>(FileType::Directory));
    inode->setFilePerm(0755);
    inode->setSizeUnit(0);
    inode->setFileSize(0);
    InodeTimestamp now;
    inode->setFmTime(now); inode->setFaTime(now); inode->setFcTime(now);

    uint64_t ino = meta_->allocate_inode(inode->file_mode.raw);
    if (ino == static_cast<uint64_t>(-1)) return false;
    inode->inode = ino;

    DirectoryLockGuard root_dir_guard(dir_lock_table_, ino, DirectoryLockMode::kExclusive);

    // 初始化 . 和 ..
    DirectoryEntry self_entry(".", inode->inode, FileType::Directory);
    DirectoryEntry parent_entry("..", inode->inode, FileType::Directory);
    if (!dir_store_->add(ino, self_entry)) return false;
    if (!dir_store_->add(ino, parent_entry)) return false;

    if (!meta_->get_inode_storage()->write_inode(ino, *inode)) return false;

    std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
    inode_table_[root_path] = ino;
    return true;
}

bool MdsServer::Mkdir(const std::string& path, mode_t mode) {
    if (path.empty() || path[0] != '/') return false;
    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos || last_slash == path.length() - 1) return false;

    std::string dirname = path.substr(last_slash + 1);
    std::string parent_path = (last_slash == 0) ? "/" : path.substr(0, last_slash);

    auto parent_ino = LookupIno(parent_path);
    if (parent_ino == static_cast<uint64_t>(-1)) return false;
    auto parent_inode = std::make_shared<Inode>();
    meta_->get_inode_storage()->read_inode(parent_ino, *parent_inode);

    {
        std::shared_lock<std::shared_mutex> lk(mtx_namespace_);
        if (inode_table_.find(path) != inode_table_.end()) return false;
    }

    DirectoryLockGuard parent_dir_guard(dir_lock_table_, parent_ino, DirectoryLockMode::kExclusive);

    auto dir_inode = std::make_shared<Inode>();
    // Store the full path in the inode's filename so that RebuildInodeTable can
    // reconstruct absolute path -> inode mappings after restart.
    dir_inode->setFilename(path);
    dir_inode->setFileType(static_cast<uint8_t>(FileType::Directory));
    dir_inode->setFilePerm(mode & 0777);
    dir_inode->setFileSize(0);
    InodeTimestamp now;
    dir_inode->setFmTime(now); dir_inode->setFaTime(now); dir_inode->setFcTime(now);

    uint64_t new_inode = meta_->allocate_inode(mode);
    if (new_inode == 0) return false;
    dir_inode->inode = new_inode;

    // 初始化 . 和 ..
    DirectoryEntry self_entry(".", dir_inode->inode, FileType::Directory);
    DirectoryEntry parent_entry("..", parent_ino, FileType::Directory);
    if (!dir_store_->add(new_inode, self_entry)) return false;
    if (!dir_store_->add(new_inode, parent_entry)) return false;

    // 在父目录加入子目录项
    DirectoryEntry new_dir_entry(dirname, new_inode, FileType::Directory);
    if (!dir_store_->add(parent_ino, new_dir_entry)) return false;

    if (!meta_->get_inode_storage()->write_inode(new_inode, *dir_inode)) return false;
    if (!meta_->get_inode_storage()->write_inode(parent_ino, *parent_inode)) return false;

    // If KV is enabled, store path -> inode mapping for the new directory
    if (meta_) {
        meta_->put_inode_for_path(path, *dir_inode);
    }

    std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
    inode_table_[path] = new_inode;
    return true;
}

bool MdsServer::Rmdir(const std::string& path) {
    auto inode_no = LookupIno(path);
    if (inode_no == static_cast<uint64_t>(-1)) return false;

    auto inode = std::make_shared<Inode>();
    if (!meta_->get_inode_storage()->read_inode(inode_no, *inode)) return false;

    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos || last_slash == path.length() - 1) return false;
    std::string dirname = path.substr(last_slash + 1);
    std::string parent_path = (last_slash == 0) ? "/" : path.substr(0, last_slash);

    auto parent_ino = LookupIno(parent_path);
    if (parent_ino == static_cast<uint64_t>(-1)) return false;

    auto parent_inode = std::make_shared<Inode>();
    meta_->get_inode_storage()->read_inode(parent_ino, *parent_inode);

    [[maybe_unused]] auto dir_locks = acquire_directory_locks(dir_lock_table_, {
        { parent_ino, DirectoryLockMode::kExclusive },
        { inode_no, DirectoryLockMode::kExclusive }
    });

    // 必须为空（仅含.和..）
    std::vector<DirectoryEntry> entries;
    if (!dir_store_->read(inode_no, entries)) return false;
    size_t non_dot = 0;
    for (auto& e : entries) {
        std::string n(e.name, e.name_len);
        if (n != "." && n != "..") ++non_dot;
    }
    if (non_dot > 0) return false;

    // 从父目录删除目录项
    if (!dir_store_->remove(parent_ino, dirname)) return false;
    if (!dir_store_->reset(inode_no)) return false;

    {
        std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
        inode_table_.erase(path);
    }
    if (meta_) {
        // remove KV mapping for this path (if present)
        meta_->delete_inode_path(path);
        meta_->mark_inode_free(inode_no);
    }
    return true;
}

bool MdsServer::CreateFile(const std::string& path, mode_t mode) {
    if ((LookupIno(path)) != static_cast<uint64_t>(-1)) return false;
    if (path.empty() || path[0] != '/') return false;
    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos || last_slash == path.length() - 1) return false;

    std::string filename = path.substr(last_slash + 1);
    std::string parent_path = (last_slash == 0) ? "/" : path.substr(0, last_slash);

    auto parent_ino = LookupIno(parent_path);
    if (parent_ino == static_cast<uint64_t>(-1)) {
        std::cerr << "[MDS] CreateFile parent missing: " << parent_path << std::endl;
        return false;
    }

    auto parent_inode = std::make_shared<Inode>();
    meta_->get_inode_storage()->read_inode(parent_ino, *parent_inode);

    DirectoryLockGuard parent_dir_guard(dir_lock_table_, parent_ino, DirectoryLockMode::kExclusive);

    auto new_inode = std::make_shared<Inode>();
    new_inode->setFileType(static_cast<uint8_t>(FileType::Regular));
    new_inode->setFilePerm(mode & 0xFFF);
    new_inode->setFmTime(InodeTimestamp());
    new_inode->setFaTime(InodeTimestamp());
    new_inode->setFcTime(InodeTimestamp());
    new_inode->setFilename(path);

    new_inode->inode = meta_->allocate_inode(mode);
    if (new_inode->inode == static_cast<uint64_t>(-1)) {
        std::cerr << "[MDS] CreateFile allocate_inode failed for " << path << std::endl;
        return false;
    }

    // 为 inode 分配卷（如果 MDS 配置了 volume allocator）
    if (volume_allocator_) {
        if (!volume_allocator_->allocate_for_inode(new_inode)) {
            std::cerr << "[MDS] CreateFile volume allocation failed for " << path << std::endl;
        }
    }

    // 在父目录加入文件目录项
    DirectoryEntry file_entry(filename, new_inode->inode, FileType::Regular);
    if (!dir_store_->add(parent_ino, file_entry)) {
        std::cerr << "[MDS] CreateFile dir_store add failed for " << path << std::endl;
        return false;
    }

    if (!meta_->get_inode_storage()->write_inode(new_inode->inode, *new_inode)) {
        std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
        inode_table_.erase(path);
        std::cerr << "[MDS] CreateFile write_inode failed for " << path << std::endl;
        return false;
    }

    // KV sync: store path -> inode mapping when creating a file
    if (meta_) {
        if (!meta_->put_inode_for_path(path, *new_inode)) {
            std::cerr << "[MDS] CreateFile KV put failed for " << path << std::endl;
        }
    }

    std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
    inode_table_[path] = new_inode->inode;
    return true;
}

bool MdsServer::RemoveFile(const std::string& path) {
    auto inode_no = LookupIno(path);
    if (inode_no == static_cast<uint64_t>(-1)) return false;

    auto inode = std::make_shared<Inode>();
    if (!meta_->get_inode_storage()->read_inode(inode_no, *inode)) return false;

    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos || last_slash == path.length() - 1) return false;
    std::string filename = path.substr(last_slash + 1);
    std::string parent_path = (last_slash == 0) ? "/" : path.substr(0, last_slash);

    auto parent_ino = LookupIno(parent_path);
    if (parent_ino == static_cast<uint64_t>(-1)) return false;

    auto parent_inode = std::make_shared<Inode>();
    meta_->get_inode_storage()->read_inode(parent_ino, *parent_inode);

    DirectoryLockGuard parent_dir_guard(dir_lock_table_, parent_ino, DirectoryLockMode::kExclusive);

    if (!dir_store_->remove(parent_ino, filename)) return false;

    bool released = false;
    if (volume_manager_) {
        released = volume_manager_->release_inode_blocks(inode);
    }
    if (!released && volume_allocator_) {
        volume_allocator_->free_blocks_for_inode(inode);
    }
    notify_handle_observer(inode->inode);

    {
        std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
        inode_table_.erase(path);
    }
    if (meta_) {
        // delete path mapping in KV and free inode
        meta_->delete_inode_path(path);
        meta_->mark_inode_free(inode_no);
    }
    // NOTE: 文件数据块释放上面已尝试执行
    return true;
}

bool MdsServer::Ls(const std::string& path) {
    auto ino = LookupIno(path);
    if (ino == static_cast<uint64_t>(-1)) return false;

    auto inode = std::make_shared<Inode>();
    if (!meta_->get_inode_storage()->read_inode(ino, *inode)) return false;

    if (inode->file_mode.fields.file_type != static_cast<uint8_t>(FileType::Directory)) return false;

    auto entries = ReadDirectoryEntries(inode);
    std::cout << "[LS] 目录: " << path << " (inode: " << ino << ")" << std::endl;
    for (auto& entry : entries) {
        std::cout << std::string(entry.name, entry.name_len)
                  << " (inode: " << entry.inode
                  << ", type: " << static_cast<int>(entry.file_type) << ")\n";
    }
    return true;
}

uint64_t MdsServer::LookupIno(const std::string& abs_path) {
    {
        std::shared_lock<std::shared_mutex> lk(mtx_namespace_);
        auto it = inode_table_.find(abs_path);
        if (it != inode_table_.end()) return it->second;
    }
    auto ptr = FindInodeByPath(abs_path);
    if (ptr) return ptr->inode;
    return static_cast<uint64_t>(-1);
}

std::shared_ptr<Inode> MdsServer::FindInodeByPath(const std::string& path) {
    // Backwards-compatible lookup:
    // 1) check in-memory inode_table_ (fast, preserves previous behavior),
    // 2) if not found and KV is enabled, consult MetadataManager KV (path -> inode),
    // 3) otherwise return nullptr.
    if (path.empty() || path[0] != '/') return nullptr;
    if (!meta_) return nullptr;

    if (path == "/") {
        const uint64_t root_inode_number = GetRootInode();
        auto root_inode = std::make_shared<Inode>();
        meta_->get_inode_storage()->read_inode(root_inode_number, *root_inode);
        return root_inode;
    }

    // 1) Try in-memory table first for fast path and compatibility with tests and legacy callers.
    {
        std::shared_lock<std::shared_mutex> lk(mtx_namespace_);
        auto it = inode_table_.find(path);
        if (it != inode_table_.end()) {
            auto inode_ptr = std::make_shared<Inode>();
            if (meta_->get_inode_storage()->read_inode(it->second, *inode_ptr)) {
                return inode_ptr;
            }
            // if read fails, fallthrough to KV lookup if available
        }
    }

    // 2) If not found in-memory, consult KV-backed path->inode index
    auto maybe = meta_->get_inode_by_path(path);
    if (maybe) {
        auto inode_ptr = std::make_shared<Inode>();
        *inode_ptr = *maybe;
        return inode_ptr;
    }

    // Not found
    return nullptr;
}

// ========== 目录项：改用 DirStore ==========

bool MdsServer::AddDirectoryEntry(const std::shared_ptr<Inode>& dir_inode, const DirectoryEntry& new_entry) {
    return dir_store_->add(dir_inode->inode, new_entry);
}

bool MdsServer::RemoveDirectoryEntry(const std::shared_ptr<Inode>& dir_inode, const std::string& name) {
    return dir_store_->remove(dir_inode->inode, name);
}

std::vector<DirectoryEntry> MdsServer::ReadDirectoryEntries(const std::shared_ptr<Inode>& dir_inode) {
    std::vector<DirectoryEntry> entries;
    DirectoryLockGuard dir_guard(dir_lock_table_, dir_inode->inode, DirectoryLockMode::kShared);
    dir_store_->read(dir_inode->inode, entries);
    return entries;
}

// ========== inode/位图 与 冷数据扫描 维持不变 ==========

uint64_t MdsServer::GetRootInode() const {
    return 2; // 暂定
}

uint64_t MdsServer::GetTotalInodes() const {
    return meta_ ? meta_->get_total_inodes() : 0;
}

bool MdsServer::IsInodeAllocated(uint64_t ino) {
    return meta_ ? meta_->is_inode_allocated(ino) : false;
}

uint64_t MdsServer::AllocateInode(mode_t mode) {
    return meta_ ? meta_->allocate_inode(mode) : 0;
}

bool MdsServer::ReadInode(uint64_t ino, Inode& out) {
    if (!meta_) return false;
    auto st = meta_->get_inode_storage();
    return st ? st->read_inode(ino, out) : false;
}

bool MdsServer::WriteInode(uint64_t ino, const Inode& in) {
    if (!meta_) return false;
    auto st = meta_->get_inode_storage();
    return st ? st->write_inode(ino, in) : false;
}

// ========== 冷数据扫描（不依赖客户端 AccessTracker，基于 atime 全量排序） ==========

std::vector<uint64_t> MdsServer::CollectColdInodes(size_t max_candidates, size_t /*min_age_windows*/) {
    std::vector<uint64_t> result;
    if (!meta_) return result;

    uint64_t total_slots = meta_->get_total_inodes();
    if (total_slots == 0) return result;

    std::vector<std::pair<uint64_t, uint32_t>> vec;
    vec.reserve(1024);

    for (uint64_t ino = 0; ino < total_slots; ++ino) {
        if (!meta_->is_inode_allocated(ino)) continue;
        Inode dinode;
        if (!meta_->get_inode_storage()->read_inode(ino, dinode)) continue;
        uint32_t key = inode_timestamp_key(dinode.fa_time);
        vec.emplace_back(ino, key);
    }

    if (vec.empty()) return result;

    std::stable_sort(vec.begin(), vec.end(),
        [](const auto& a, const auto& b){ return a.second < b.second; });

    size_t pick = std::min(max_candidates, vec.size());
    result.reserve(pick);
    for (size_t i = 0; i < pick; ++i) result.push_back(vec[i].first);
    return result;
}

std::shared_ptr<boost::dynamic_bitset<>> MdsServer::CollectColdInodesBitmap(size_t min_age_windows) {
    if (!meta_) return nullptr;
    uint64_t total_slots = meta_->get_total_inodes();
    if (total_slots == 0) return nullptr;

    auto cold_bitmap = std::make_shared<boost::dynamic_bitset<>>(static_cast<size_t>(total_slots));

    // 简单策略：按 atime 排序，选择最老的 percent% 标记为冷
    double percent = std::clamp<double>(min_age_windows * 20.0, 0.0, 100.0); // 可调整策略
    auto oldest_list = CollectColdInodesByAtimePercent(percent);
    for (auto ino : oldest_list) {
        if (ino < total_slots) cold_bitmap->set(static_cast<size_t>(ino));
    }
    return cold_bitmap;
}

std::vector<uint64_t> MdsServer::CollectColdInodesByAtimePercent(double percent) {
    std::vector<uint64_t> result;
    if (!meta_) return result;
    if (percent <= 0.0) return result;

    uint64_t total_slots = meta_->get_total_inodes();
    if (total_slots == 0) return result;

    std::vector<std::pair<uint64_t, uint32_t>> vec;
    vec.reserve(1024);

    for (uint64_t ino = 0; ino < total_slots; ++ino) {
        if (!meta_->is_inode_allocated(ino)) continue;
        Inode dinode;
        if (!meta_->get_inode_storage()->read_inode(ino, dinode)) continue;
        uint32_t key = inode_timestamp_key(dinode.fa_time);
        vec.emplace_back(ino, key);
    }

    if (vec.empty()) return result;

    std::stable_sort(vec.begin(), vec.end(),
        [](const auto& a, const auto& b){ return a.second < b.second; });

    size_t total = vec.size();
    size_t pick = static_cast<size_t>(std::ceil((percent / 100.0) * static_cast<double>(total)));
    if (pick == 0 && percent > 0.0) pick = 1;
    if (pick > total) pick = total;

    result.reserve(pick);
    for (size_t i = 0; i < pick; ++i) result.push_back(vec[i].first);
    return result;
}

// ========== 工具 ==========

void MdsServer::RebuildInodeTable() {
    std::unordered_map<std::string, uint64_t> rebuilt;
    if (meta_) {
        auto inode_storage = meta_->get_inode_storage();
        uint64_t inode_count = meta_->get_total_inodes();

        for (uint64_t i = 0; i < inode_count; ++i) {
            if (!meta_->is_inode_allocated(i)) continue;
            Inode inode;
            if (!inode_storage->read_inode(i, inode)) continue;
            if (inode.filename.empty()) continue;
            rebuilt[inode.filename] = inode.inode;
        }
    }

    size_t rebuilt_size = rebuilt.size();
    {
        std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
        inode_table_ = std::move(rebuilt);
    }
    std::cout << "[MDS] inode_table 重建完成，文件数: " << rebuilt_size << std::endl;
}

void MdsServer::ClearInodeTable() {
    std::unique_lock<std::shared_mutex> lk(mtx_namespace_);
    inode_table_.clear();
}

bool MdsServer::TruncateFile(const std::string& path) {
    auto inode = FindInodeByPath(path);
    if (!inode) return false;
    bool released = false;
    if (volume_manager_) {
        released = volume_manager_->release_inode_blocks(inode);
    }
    if (!released && volume_allocator_) {
        volume_allocator_->free_blocks_for_inode(inode);
        inode->clearBlocks();
    }
    inode->setSizeUnit(0);
    inode->setFileSize(0);

    InodeTimestamp now;
    inode->setFmTime(now);
    inode->setFaTime(now);
    inode->setFcTime(now);

    notify_handle_observer(inode->inode);
    return meta_ && meta_->get_inode_storage()
        && meta_->get_inode_storage()->write_inode(inode->inode, *inode);
}

void MdsServer::notify_handle_observer(uint64_t inode) {
    if (auto observer = handle_observer_.lock()) {
        observer->CloseHandlesForInode(inode);
    }
}
