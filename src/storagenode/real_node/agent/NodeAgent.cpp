#include "NodeAgent.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {

uint64_t NowMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

} // namespace

NodeAgent::NodeAgent(std::string srm_addr,
                     uint32_t listen_port,
                     std::shared_ptr<DiskManager> disk_mgr,
                     std::string advertise_ip,
                     std::string hostname_override,
                     int heartbeat_interval_ms,
                     int register_backoff_ms)
    : srm_addr_(std::move(srm_addr)),
      listen_port_(listen_port),
      disk_mgr_(std::move(disk_mgr)),
      advertise_ip_(std::move(advertise_ip)),
      hostname_override_(std::move(hostname_override)),
      heartbeat_interval_ms_(heartbeat_interval_ms),
      register_backoff_ms_(register_backoff_ms) {}

NodeAgent::~NodeAgent() {
    Stop();
}

void NodeAgent::Start() {
    if (running_.exchange(true)) {
        return;
    }
    worker_ = std::thread([this]() { Run(); });
}

void NodeAgent::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (worker_.joinable()) {
        worker_.join();
    }
}

void NodeAgent::Run() {
    while (running_) {
        if (node_id_.empty()) {
            if (!DoRegister()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(register_backoff_ms_));
                continue;
            }
        } else {
            if (!DoHeartbeat()) {
                // if SRM asked for re-reg, node_id_ will be cleared inside DoHeartbeat
                if (node_id_.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(register_backoff_ms_));
                    continue;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval_ms_));
    }
}

bool NodeAgent::DoRegister() {
    if (srm_addr_.empty()) {
        std::cerr << "[NodeAgent] srm_addr is empty; skip registration" << std::endl;
        return false;
    }
    if (!channel_) {
        channel_ = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions opts;
        opts.timeout_ms = 3000;
        opts.max_retry = 1;
        if (channel_->Init(srm_addr_.c_str(), &opts) != 0) {
            std::cerr << "[NodeAgent] Failed to init channel to SRM at " << srm_addr_ << std::endl;
            channel_.reset();
            return false;
        }
        stub_ = std::make_unique<storagenode::ClusterManagerService_Stub>(channel_.get());
    }
    if (!stub_) {
        return false;
    }

    storagenode::RegisterRequest req;
    storagenode::RegisterResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    req.set_ip(advertise_ip_.empty() ? "127.0.0.1" : advertise_ip_);
    req.set_port(listen_port_);
    req.set_hostname(hostname_override_.empty() ? ResolveHostname() : hostname_override_);
    FillDiskInfo(&req);

    stub_->RegisterNode(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "[NodeAgent] RegisterNode RPC failed: " << cntl.ErrorText() << std::endl;
        return false;
    }
    if (resp.status().code() != 0) {
        std::cerr << "[NodeAgent] RegisterNode rejected: " << resp.status().message() << std::endl;
        return false;
    }
    node_id_ = resp.node_id();
    return true;
}

bool NodeAgent::DoHeartbeat() {
    if (!stub_) {
        return false;
    }
    storagenode::HeartbeatRequest req;
    storagenode::HeartbeatResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);

    req.set_node_id(node_id_);
    req.set_timestamp_ms(NowMs());
    // TODO: add real metrics when available
    req.set_cpu_usage(0.0);
    req.set_mem_usage(0.0);
    req.set_in_flight_io(0);

    stub_->Heartbeat(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return false;
    }
    if (resp.status().code() != 0) {
        if (resp.require_rereg()) {
            node_id_.clear();
        }
        return false;
    }
    if (resp.require_rereg()) {
        node_id_.clear();
    }
    return true;
}

void NodeAgent::FillDiskInfo(storagenode::RegisterRequest* req) {
    if (!req || !disk_mgr_) {
        return;
    }
    DiskStats stats = disk_mgr_->Stats();
    auto* disk = req->add_disks();
    disk->set_mount_point(disk_mgr_->mount_point());
    disk->set_total_bytes(stats.total_bytes);
    disk->set_free_bytes(stats.free_bytes);
}

std::string NodeAgent::ResolveHostname() const {
#ifdef _WIN32
    CHAR buf[MAX_COMPUTERNAME_LENGTH + 1];
    DWORD size = sizeof(buf);
    if (GetComputerNameA(buf, &size)) {
        return std::string(buf, size);
    }
    return "windows-host";
#else
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf);
    }
    return "real-node";
#endif
}
