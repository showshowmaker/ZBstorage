#include "common.pb.h"

namespace rpc {

Status Status::Ok() { return Status{0, {}}; }

Status Status::Error(int c, const std::string& msg) { return Status{c, msg}; }

} // namespace rpc
