#pragma once

#include <string>

#include "msg/RPC/proto/rpc_common.pb.h"

namespace StatusUtils {

rpc::StatusCode FromErrno(int err);
rpc::StatusCode NormalizeCode(int code);
void SetStatus(rpc::Status* status, rpc::StatusCode code, const std::string& msg = "");

} // namespace StatusUtils
