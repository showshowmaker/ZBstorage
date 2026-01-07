#pragma once

#include <random>
#include <string>

#include "SimulationConfig.h"
#include "storage_node.pb.h"

class VirtualNodeEngine {
public:
    explicit VirtualNodeEngine(SimulationConfig cfg);

    void SimulateWrite(const storagenode::WriteRequest* req,
                       storagenode::WriteReply* resp);

    void SimulateRead(const storagenode::ReadRequest* req,
                      storagenode::ReadReply* resp);

    void SimulateTruncate(const storagenode::TruncateRequest* req,
                          storagenode::TruncateReply* resp);

private:
    void MaybeFail(rpc::Status* status);
    void AddLatency();
    void FillStatus(rpc::Status* status, rpc::StatusCode code, const std::string& msg);

    SimulationConfig cfg_;
    std::mt19937 rng_;
    std::uniform_int_distribution<int> latency_dist_;
    std::uniform_real_distribution<double> failure_dist_;
};
