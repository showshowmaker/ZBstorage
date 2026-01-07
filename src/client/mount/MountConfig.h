#pragma once

#include <string>

struct MountConfig {
    std::string mds_addr{"127.0.0.1:9000"};
    std::string srm_addr{"127.0.0.1:9100"};
    std::string mount_point{"/mnt/zbstorage"};
    std::string default_node_id{"node-1"};
    int rpc_timeout_ms{3000};
    int rpc_max_retry{2};
};
