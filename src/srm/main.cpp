#include <brpc/server.h>
#include <gflags/gflags.h>

#include <chrono>
#include <iostream>
#include <memory>

#include "ClusterManagerServiceImpl.h"
#include "StorageNodeManager.h"
#include "gateway/GatewayServiceImpl.h"
#include "gateway/RequestDispatcher.h"
#include "simulation/VirtualNodeEngine.h"
#include "simulation/SimulationConfig.h"

DEFINE_int32(srm_port, 9100, "Port for SRM cluster manager service");
DEFINE_int32(heartbeat_timeout_sec, 30, "Heartbeat timeout in seconds");
DEFINE_int32(health_check_interval_sec, 10, "Health monitor interval in seconds");
DEFINE_int32(virtual_min_latency_ms, 5, "Virtual node min latency (ms)");
DEFINE_int32(virtual_max_latency_ms, 50, "Virtual node max latency (ms)");
DEFINE_double(virtual_failure_rate, 0.0, "Virtual node failure rate (0.0-1.0)");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto manager = std::make_shared<StorageNodeManager>(
        std::chrono::seconds(FLAGS_heartbeat_timeout_sec),
        std::chrono::seconds(FLAGS_health_check_interval_sec));
    manager->Start();

    ClusterManagerServiceImpl service(manager);
    SimulationConfig sim_cfg;
    sim_cfg.min_latency_ms = FLAGS_virtual_min_latency_ms;
    sim_cfg.max_latency_ms = FLAGS_virtual_max_latency_ms;
    sim_cfg.failure_rate = FLAGS_virtual_failure_rate;
    auto virtual_engine = std::make_shared<VirtualNodeEngine>(sim_cfg);
    auto dispatcher = std::make_shared<RequestDispatcher>(manager, virtual_engine);
    GatewayServiceImpl gateway(dispatcher);

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
