#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "storage_node.pb.h"

DEFINE_string(server, "127.0.0.1:9010", "Storage real node server address");
DEFINE_uint64(chunk_id, 1, "Chunk id to read/write");
DEFINE_uint64(offset, 0, "Offset for read/write");
DEFINE_string(data, "hello real node", "Data to write");

static void PrintStatus(const rpc::Status& st, const std::string& api) {
    if (st.code() == 0) {
        std::cout << api << " status=OK" << std::endl;
    } else {
        std::cout << api << " status=" << st.code() << " msg=" << st.message() << std::endl;
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Channel channel;
    brpc::ChannelOptions opts;
    if (channel.Init(FLAGS_server.c_str(), &opts) != 0) {
        std::cerr << "Failed to init channel to " << FLAGS_server << std::endl;
        return -1;
    }

    storagenode::StorageService_Stub stub(&channel);

    // Write request
    {
        storagenode::WriteRequest req;
        storagenode::WriteReply resp;
        brpc::Controller cntl;
        req.set_chunk_id(FLAGS_chunk_id);
        req.set_offset(FLAGS_offset);
        req.set_data(FLAGS_data);
        req.set_checksum(0); // skip checksum validation

        stub.Write(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "RPC Write failed: " << cntl.ErrorText() << std::endl;
            return -1;
        }
        PrintStatus(resp.status(), "Write");
        if (resp.status().code() != 0) {
            return -1;
        }
        std::cout << "bytes_written=" << resp.bytes_written() << std::endl;
    }

    // Read back
    {
        storagenode::ReadRequest req;
        storagenode::ReadReply resp;
        brpc::Controller cntl;
        req.set_chunk_id(FLAGS_chunk_id);
        req.set_offset(FLAGS_offset);
        req.set_length(static_cast<uint64_t>(FLAGS_data.size()));

        stub.Read(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "RPC Read failed: " << cntl.ErrorText() << std::endl;
            return -1;
        }
        PrintStatus(resp.status(), "Read");
        if (resp.status().code() != 0) {
            return -1;
        }
        std::cout << "bytes_read=" << resp.bytes_read()
                  << " data=" << resp.data() << std::endl;
    }

    return 0;
}
