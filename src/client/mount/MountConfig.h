#pragma once

#include <string>

struct MountConfig {
    std::string mds_addr{"127.0.0.1:9000"};
    std::string vfs_addr{"127.0.0.1:9001"};
    std::string mount_point{"/mnt/zbstorage"};
    int rpc_timeout_ms{3000};
    int rpc_max_retry{2};
};
