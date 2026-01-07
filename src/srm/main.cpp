#include <brpc/server.h>
#include <gflags/gflags.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <cstdint>

#include "ClusterManagerServiceImpl.h"
#include "StorageNodeManager.h"
#include "gateway/GatewayServiceImpl.h"
#include "gateway/RequestDispatcher.h"
#include "simulation/VirtualNodeEngine.h"
#include "simulation/SimulationConfig.h"
#include "common/LogRedirect.h"

DEFINE_int32(srm_port, 9100, "Port for SRM cluster manager service");
DEFINE_string(mds_addr, "127.0.0.1:9000", "MDS service address for volume registration");
DEFINE_int32(heartbeat_timeout_sec, 30, "Heartbeat timeout in seconds");
DEFINE_int32(health_check_interval_sec, 10, "Health monitor interval in seconds");
DEFINE_int32(virtual_min_latency_ms, 5, "Virtual node min latency (ms)");
DEFINE_int32(virtual_max_latency_ms, 50, "Virtual node max latency (ms)");
DEFINE_double(virtual_failure_rate, 0.0, "Virtual node failure rate (0.0-1.0)");
DEFINE_int32(virtual_node_count, 0, "Number of virtual nodes to pre-register");
DEFINE_uint64(virtual_node_capacity_bytes, 100ULL * 1024 * 1024 * 1024, "Capacity per virtual node in bytes");
DEFINE_string(log_file, "", "Log file path (append). Empty = stdout/stderr");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!RedirectLogs(FLAGS_log_file)) {
        std::cerr << "Failed to open log file: " << FLAGS_log_file << std::endl;
        return -1;
    }

    auto manager = std::make_shared<StorageNodeManager>(
        std::chrono::seconds(FLAGS_heartbeat_timeout_sec),
        std::chrono::seconds(FLAGS_health_check_interval_sec),
        FLAGS_mds_addr);
    manager->Start();

    ClusterManagerServiceImpl service(manager);
    SimulationConfig sim_cfg;
    sim_cfg.min_latency_ms = FLAGS_virtual_min_latency_ms;
    sim_cfg.max_latency_ms = FLAGS_virtual_max_latency_ms;
    sim_cfg.failure_rate = FLAGS_virtual_failure_rate;
    auto virtual_engine = std::make_shared<VirtualNodeEngine>(sim_cfg);
    auto dispatcher = std::make_shared<RequestDispatcher>(manager, virtual_engine);
    GatewayServiceImpl gateway(dispatcher);

    // Pre-register virtual nodes if requested.
    for (int i = 0; i < FLAGS_virtual_node_count; ++i) {
        const std::string vnode_id = "vnode-" + std::to_string(i + 1);
        manager->AddVirtualNode(vnode_id, SimulationParams{}, FLAGS_virtual_node_capacity_bytes);
    }

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add ClusterManagerService" << std::endl;
        return -1;
    }
    if (server.AddService(&gateway, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add GatewayService" << std::endl;
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;

    if (server.Start(FLAGS_srm_port, &options) != 0) {
        std::cerr << "Failed to start SRM server on port " << FLAGS_srm_port << std::endl;
        return -1;
    }

    std::cout << "SRM ClusterManagerService started on port " << FLAGS_srm_port
              << " heartbeat_timeout_sec=" << FLAGS_heartbeat_timeout_sec
              << " health_check_interval_sec=" << FLAGS_health_check_interval_sec
              << "; Gateway StorageService on same port"
              << std::endl;
    server.RunUntilAskedToQuit();
    manager->Stop();
    return 0;
}
