#define FUSE_USE_VERSION 29
#include <fuse.h>
#include <gflags/gflags.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "client/mount/DfsClient.h"
#include "client/mount/MountConfig.h"

DEFINE_string(mds_addr, "127.0.0.1:9000", "MDS service address");
DEFINE_string(srm_addr, "127.0.0.1:9100", "SRM Gateway service address");
DEFINE_string(node_id, "node-1", "Default node id to target on SRM/Real Node");
DEFINE_string(mount_point, "/mnt/zbstorage", "Mount point");
DEFINE_bool(allow_other, false, "Pass -o allow_other to FUSE so non-root users can access");
DEFINE_bool(foreground, false, "Run FUSE in foreground (pass -f)");

namespace {

std::shared_ptr<DfsClient> g_client;

int fuse_getattr_cb(const char* path, struct stat* stbuf) {
    if (!g_client) return -ECOMM;
    return g_client->GetAttr(path, stbuf);
}

int fuse_readdir_cb(const char* path, void* buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info* fi) {
    (void)offset;
    (void)fi;
    if (!g_client) return -ECOMM;
    return g_client->ReadDir(path, buf, filler);
}

int fuse_open_cb(const char* path, struct fuse_file_info* fi) {
    if (!g_client) return -ECOMM;
    int fd = -1;
    int rc = g_client->Open(path, fi->flags, fd);
    if (rc == 0) {
        fi->fh = static_cast<uint64_t>(fd);
    }
    return rc;
}

int fuse_create_cb(const char* path, mode_t mode, struct fuse_file_info* fi) {
    if (!g_client) return -ECOMM;
    int fd = -1;
    int rc = g_client->Create(path, fi->flags, mode, fd);
    if (rc == 0) {
        fi->fh = static_cast<uint64_t>(fd);
    }
    return rc;
}

int fuse_read_cb(const char* path, char* buf, size_t size, off_t offset,
                 struct fuse_file_info* fi) {
    (void)path;
    if (!g_client) return -ECOMM;
    ssize_t bytes = 0;
    int rc = g_client->Read(static_cast<int>(fi->fh), buf, size, offset, bytes);
    if (rc != 0) return rc;
    return static_cast<int>(bytes);
}

int fuse_write_cb(const char* path, const char* buf, size_t size, off_t offset,
                  struct fuse_file_info* fi) {
    (void)path;
    if (!g_client) return -ECOMM;
    ssize_t bytes = 0;
    int rc = g_client->Write(static_cast<int>(fi->fh), buf, size, offset, bytes);
    if (rc != 0) return rc;
    return static_cast<int>(bytes);
}

int fuse_release_cb(const char* path, struct fuse_file_info* fi) {
    (void)path;
    if (!g_client) return -ECOMM;
    return g_client->Close(static_cast<int>(fi->fh));
}

struct fuse_operations BuildFuseOps() {
    struct fuse_operations ops {};
    ops.getattr = fuse_getattr_cb;
    ops.readdir = fuse_readdir_cb;
    ops.open = fuse_open_cb;
    ops.read = fuse_read_cb;
    ops.write = fuse_write_cb;
    ops.release = fuse_release_cb;
    ops.create = fuse_create_cb;
    return ops;
}

} // namespace

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    MountConfig cfg;
    cfg.mds_addr = FLAGS_mds_addr;
    cfg.srm_addr = FLAGS_srm_addr;
    cfg.mount_point = FLAGS_mount_point;
    cfg.default_node_id = FLAGS_node_id;
    g_client = std::make_shared<DfsClient>(cfg);
    if (!g_client->Init()) {
        std::fprintf(stderr, "Failed to initialize DFS client (mds=%s srm=%s)\n",
                     cfg.mds_addr.c_str(), cfg.srm_addr.c_str());
        return 1;
    }

    struct fuse_operations ops = BuildFuseOps();

    std::printf("Mounting ZBStorage at %s (MDS=%s, SRM=%s, node_id=%s)\n",
                cfg.mount_point.c_str(), cfg.mds_addr.c_str(), cfg.srm_addr.c_str(),
                cfg.default_node_id.c_str());

    // Build FUSE argv. Always pass program name and mount point.
    // Optionally pass "-o allow_other" and "-f" (foreground) when enabled.
    char* fuse_argv[6];
    fuse_argv[0] = argv[0];
    fuse_argv[1] = const_cast<char*>(cfg.mount_point.c_str());
    int fuse_argc = 2;
    if (FLAGS_allow_other) {
        fuse_argv[fuse_argc++] = const_cast<char*>("-o");
        fuse_argv[fuse_argc++] = const_cast<char*>("allow_other");
    }
    if (FLAGS_foreground) {
        fuse_argv[fuse_argc++] = const_cast<char*>("-f");
    }

    return fuse_main(fuse_argc, fuse_argv, &ops, nullptr);
}
