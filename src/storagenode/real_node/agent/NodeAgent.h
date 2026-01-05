#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include "cluster_manager.pb.h"
#include "../io/DiskManager.h"

class NodeAgent {
public:
    NodeAgent(std::string srm_addr,
              uint32_t listen_port,
              std::shared_ptr<DiskManager> disk_mgr,
              std::string advertise_ip,
              std::string hostname_override,
              int heartbeat_interval_ms,
              int register_backoff_ms);
    ~NodeAgent();

    NodeAgent(const NodeAgent&) = delete;
    NodeAgent& operator=(const NodeAgent&) = delete;

    void Start();
    void Stop();

private:
    void Run();
    bool DoRegister();
    bool DoHeartbeat();
    void FillDiskInfo(storagenode::RegisterRequest* req);
    std::string ResolveHostname() const;

    const std::string srm_addr_;
    const uint32_t listen_port_;
    std::shared_ptr<DiskManager> disk_mgr_;
    const std::string advertise_ip_;
    const std::string hostname_override_;
    const int heartbeat_interval_ms_;
    const int register_backoff_ms_;

    std::unique_ptr<brpc::Channel> channel_;
    std::unique_ptr<storagenode::ClusterManagerService_Stub> stub_;

    std::thread worker_;
    std::atomic<bool> running_{false};
    std::string node_id_;
};
