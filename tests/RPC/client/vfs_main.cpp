// src/client/vfs_main.cpp
#include <iostream>
#include <string>
#include <vector>

#include "client/VFS_dist.h"

int main(int argc, char* argv[]) {
    std::string mds_addr = "127.0.0.1:8001";
    if (argc > 1) {
        mds_addr = argv[1];  // 允许命令行指定 MDS 地址
    }

    try {
        DistFileSystem fs(mds_addr);

        const std::string path = "/data.bin";
        int fd = fs.open(path, MO_RDWR | MO_CREAT, 0644);
        if (fd < 0) {
            std::cerr << "[client] open " << path << " failed\n";
            return 1;
        }

        const std::string payload = "hello distributed vfs";
        std::cout << "[client] write \"" << payload << "\"\n";
        if (fs.write(fd, payload.data(), payload.size()) !=
            static_cast<ssize_t>(payload.size())) {
            std::cerr << "[client] write failed\n";
            return 2;
        }

        if (fs.seek(fd, 0, SEEK_SET) < 0) {
            std::cerr << "[client] seek failed\n";
            return 3;
        }

        std::vector<char> buf(payload.size());
        ssize_t n = fs.read(fd, buf.data(), buf.size());
        if (n != static_cast<ssize_t>(payload.size())) {
            std::cerr << "[client] read size mismatch, n=" << n << "\n";
            return 4;
        }

        std::string readback(buf.begin(), buf.end());
        std::cout << "[client] read back \"" << readback << "\"\n";

        if (readback != payload) {
            std::cerr << "[client] content mismatch\n";
            return 5;
        }

        fs.close(fd);
        std::cout << "[client] distributed vfs basic test passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[client] exception: " << e.what() << "\n";
        return -1;
    }
}
