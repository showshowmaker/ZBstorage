#pragma once

#include <string>

#include "rpc_common.pb.h"

// Helper to bridge system errno and proto StatusCode.
class StatusUtils {
public:
    // Map a system errno (or negative return) to StatusCode.
    static rpc::StatusCode FromErrno(int sys_errno);

    // Normalize a code coming from downstream (either StatusCode or errno).
    static rpc::StatusCode NormalizeCode(int code);

    // Populate rpc::Status with code/message.
    static void SetStatus(rpc::Status* status, rpc::StatusCode code, const std::string& msg = "");
};
