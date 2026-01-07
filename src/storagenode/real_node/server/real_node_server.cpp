#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <vector>

#include "StorageServiceImpl.h"
#include "../io/DiskManager.h"
#include "../io/IOEngine.h"
#include "../meta/LocalMetadataManager.h"
#include "../agent/NodeAgent.h"
#include "common/LogRedirect.h"

DEFINE_int32(port, 9010, "Port for storage real node server");
DEFINE_string(device_path, "", "Block device path (e.g. /dev/sdb), optional if already mounted");
DEFINE_string(mount_point, "/tmp/realnode_disk", "Mount point or existing data directory");
DEFINE_string(fs_type, "ext4", "Filesystem type used when auto-mounting");
DEFINE_bool(auto_mount, false, "Whether to auto-mount device_path to mount_point");
DEFINE_bool(sync_on_write, false, "Whether to fsync after writes");
DEFINE_bool(skip_mount, false, "Skip mounting/device checks and use mount_point/base_path directly");
DEFINE_string(base_path, "", "Data root; default uses mount_point if empty");
DEFINE_string(srm_addr, "", "SRM ClusterManagerService address host:port for registration/heartbeat");
DEFINE_string(advertise_ip, "", "IP address to advertise to SRM (defaults to 127.0.0.1 if empty)");
DEFINE_string(agent_hostname, "", "Optional hostname override reported to SRM");
DEFINE_int32(agent_heartbeat_ms, 3000, "Heartbeat interval in milliseconds");
DEFINE_int32(agent_register_backoff_ms, 5000, "Backoff between failed registrations in milliseconds");
DEFINE_string(log_file, "", "Log file path (append). Empty = stdout/stderr");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!RedirectLogs(FLAGS_log_file)) {
        std::cerr << "Failed to open log file: " << FLAGS_log_file << std::endl;
        return -1;
    }

    DiskMountConfig cfg;
    cfg.device_path = FLAGS_device_path;
    cfg.mount_point = FLAGS_mount_point;
    cfg.fs_type = FLAGS_fs_type;
    cfg.auto_mount = FLAGS_skip_mount ? false : FLAGS_auto_mount;
    cfg.skip_mount = FLAGS_skip_mount;

    auto disk_mgr = std::make_shared<DiskManager>(cfg);
    IOEngine::Options io_opts;
    io_opts.sync_on_write = FLAGS_sync_on_write;
    std::string data_root = FLAGS_base_path.empty() ? FLAGS_mount_point : FLAGS_base_path;
    auto io_engine = std::make_shared<IOEngine>(data_root, io_opts);
    auto metadata_mgr = std::make_shared<LocalMetadataManager>(std::vector<std::string>{data_root});

    StorageServiceImpl service(disk_mgr, metadata_mgr, io_engine);
    std::unique_ptr<NodeAgent> agent;
    if (!FLAGS_srm_addr.empty()) {
        agent = std::make_unique<NodeAgent>(FLAGS_srm_addr,
                                            static_cast<uint32_t>(FLAGS_port),
                                            disk_mgr,
                                            FLAGS_advertise_ip,
                                            FLAGS_agent_hostname,
                                            FLAGS_agent_heartbeat_ms,
                                            FLAGS_agent_register_backoff_ms);
    }

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

    if (agent) {
        agent->Start();
    }

    std::cout << "Storage real node server started at port " << FLAGS_port
              << ", base_path=" << (FLAGS_base_path.empty() ? FLAGS_mount_point : FLAGS_base_path)
              << ", skip_mount=" << (FLAGS_skip_mount ? "true" : "false")
              << ", srm_addr=" << (FLAGS_srm_addr.empty() ? "<disabled>" : FLAGS_srm_addr)
              << std::endl;
    server.RunUntilAskedToQuit();
    if (agent) {
        agent->Stop();
    }
    return 0;
}
