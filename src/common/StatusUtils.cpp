#include "StatusUtils.h"

#include <cerrno>
#include <cstring>

rpc::StatusCode StatusUtils::FromErrno(int err) {
    if (err == 0) return rpc::STATUS_SUCCESS;
    switch (err) {
        case EINVAL: return rpc::STATUS_INVALID_ARGUMENT;
        case ENOENT: return rpc::STATUS_NODE_NOT_FOUND;
        case EIO: return rpc::STATUS_IO_ERROR;
#ifdef ETIMEDOUT
        case ETIMEDOUT: return rpc::STATUS_NETWORK_ERROR;
#endif
#ifdef ECONNREFUSED
        case ECONNREFUSED: return rpc::STATUS_NETWORK_ERROR;
#endif
#ifdef ENETUNREACH
        case ENETUNREACH: return rpc::STATUS_NETWORK_ERROR;
#endif
        default: return rpc::STATUS_UNKNOWN_ERROR;
    }
}

rpc::StatusCode StatusUtils::NormalizeCode(int code) {
    switch (code) {
        case rpc::STATUS_SUCCESS:
        case rpc::STATUS_UNKNOWN_ERROR:
        case rpc::STATUS_INVALID_ARGUMENT:
        case rpc::STATUS_NODE_NOT_FOUND:
        case rpc::STATUS_IO_ERROR:
        case rpc::STATUS_NETWORK_ERROR:
        case rpc::STATUS_VIRTUAL_NODE_ERROR:
            return static_cast<rpc::StatusCode>(code);
        default:
            return FromErrno(code);
    }
}

void StatusUtils::SetStatus(rpc::Status* status, rpc::StatusCode code, const std::string& msg) {
    if (!status) return;
    status->set_code(code);
    if (code == rpc::STATUS_SUCCESS) {
        status->set_message("");
    } else {
        status->set_message(msg.empty() ? "error" : msg);
    }
}
