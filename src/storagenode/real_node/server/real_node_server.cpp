#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>
#include <memory>

#include "StorageServiceImpl.h"
#include "../io/DiskManager.h"
#include "../io/IOEngine.h"

DEFINE_int32(port, 9010, "Port for storage real node server");
DEFINE_string(device_path, "", "Block device path (e.g. /dev/sdb), optional if already mounted");
DEFINE_string(mount_point, "/tmp/realnode_disk", "Mount point or existing data directory");
DEFINE_string(fs_type, "ext4", "Filesystem type used when auto-mounting");
DEFINE_bool(auto_mount, false, "Whether to auto-mount device_path to mount_point");
DEFINE_bool(sync_on_write, false, "Whether to fsync after writes");
DEFINE_string(base_path, "", "Data root; default uses mount_point if empty");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DiskMountConfig cfg;
    cfg.device_path = FLAGS_device_path;
    cfg.mount_point = FLAGS_mount_point;
    cfg.fs_type = FLAGS_fs_type;
    cfg.auto_mount = FLAGS_auto_mount;

    auto disk_mgr = std::make_shared<DiskManager>(cfg);
    IOEngine::Options io_opts;
    io_opts.sync_on_write = FLAGS_sync_on_write;
    auto io_engine = std::make_shared<IOEngine>(FLAGS_base_path.empty() ? FLAGS_mount_point : FLAGS_base_path,
                                                io_opts);

    StorageServiceImpl service(disk_mgr, io_engine);

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add StorageService" << std::endl;
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start server on port " << FLAGS_port << std::endl;
        return -1;
    }

    std::cout << "Storage real node server started at port " << FLAGS_port
              << ", base_path=" << (FLAGS_base_path.empty() ? FLAGS_mount_point : FLAGS_base_path)
              << std::endl;
    server.RunUntilAskedToQuit();
    return 0;
}
