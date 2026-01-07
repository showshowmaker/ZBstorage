#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include "vfs.pb.h"
#include "rpc_common.pb.h"
#include "../../../src/mds/inode/inode.h"
#include <cstdio>

DEFINE_string(vfs_addr, "127.0.0.1:8012", "vfs server address");

namespace {

std::filesystem::path make_temp_dir() {
    auto base = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dir = base / ("vfs_rpc_test_" + std::to_string(stamp));
    std::filesystem::create_directories(dir);
    return dir;
}

struct TempDir {
    std::filesystem::path path;
    TempDir() : path(make_temp_dir()) {}
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        if (ec) {
            std::cerr << "Failed to remove temp directory " << path << ": " << ec.message() << std::endl;
        }
    }
};

bool expect(bool cond, const std::string& msg) {
    std::cout << "[TEST] " << msg << " -> " << (cond ? "OK" : "FAIL") << std::endl;
    if (!cond) {
        std::cerr << "    expected: success" << std::endl;
    }
    return cond;
}

} // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    if (channel.Init(FLAGS_vfs_addr.c_str(), &options) != 0) {
        std::cerr << "Fail to init channel to vfs" << std::endl;
        return -1;
    }
    rpc::VfsService_Stub stub(&channel);

    TempDir tmp;
    std::cout << "RPC VFS test tempdir: " << tmp.path << std::endl;

    rpc::Empty empty;
    rpc::Status st;
    bool all_ok = true;
    brpc::Controller c0;
    stub.Startup(&c0, &empty, &st, nullptr);
    all_ok = expect(st.code() == 0, "startup") && all_ok;

    brpc::Controller c1;
    stub.CreateRootDirectory(&c1, &empty, &st, nullptr);
    all_ok = expect(st.code() == 0, "create_root_directory") && all_ok;

    // mkdir /test
    rpc::PathModeRequest pmk;
    pmk.set_path("/test");
    pmk.set_mode(0755);
    brpc::Controller c2;
    stub.Mkdir(&c2, &pmk, &st, nullptr);
    all_ok = expect(st.code() == 0, "mkdir /test") && all_ok;
    rpc::PathRequest pls;
    pls.set_path("/");
    rpc::DirectoryListReply ls_reply;
    brpc::Controller c3;
    stub.Ls(&c3, &pls, &ls_reply, nullptr);
    all_ok = expect(ls_reply.status().code() == 0, "ls root") && all_ok;
    rpc::LookupReply lookup;
    rpc::PathRequest plook;
    plook.set_path("/test");
    brpc::Controller c4;
    stub.LookupInode(&c4, &plook, &lookup, nullptr);
    all_ok = expect(lookup.inode() != static_cast<uint64_t>(-1), "lookup_inode test") && all_ok;
    rpc::PathRequest prmdir;
    prmdir.set_path("/test");
    brpc::Controller c5;
    stub.Rmdir(&c5, &prmdir, &st, nullptr);
    all_ok = expect(st.code() == 0, "rmdir /test") && all_ok;

    rpc::PathModeRequest pmkdir;
    pmkdir.set_path("/io");
    pmkdir.set_mode(0755);
    brpc::Controller c6;
    stub.Mkdir(&c6, &pmkdir, &st, nullptr);
    all_ok = expect(st.code() == 0, "mkdir /io") && all_ok;
    rpc::PathModeRequest pcreate;
    pcreate.set_path("/io/data.bin");
    pcreate.set_mode(0644);
    brpc::Controller c7;
    stub.CreateFile(&c7, &pcreate, &st, nullptr);
    all_ok = expect(st.code() == 0, "create_file data") && all_ok;

    rpc::OpenRequest open_req;
    open_req.set_path("/io/data.bin");
    open_req.set_flags(MO_RDWR | MO_CREAT);
    open_req.set_mode(0644);
    rpc::IOReplyFD open_reply;
    brpc::Controller c8;
    stub.Open(&c8, &open_req, &open_reply, nullptr);
    int fd = static_cast<int>(open_reply.bytes());
    all_ok = expect(fd >= 0, "open data") && all_ok;

    const std::string payload = "hello vfs_new";
    rpc::IORequestFD wreq;
    wreq.set_fd(fd);
    wreq.set_data(payload);
    rpc::IOReplyFD wrep;
    brpc::Controller c9;
    stub.Write(&c9, &wreq, &wrep, nullptr);
    all_ok = expect(wrep.bytes() == static_cast<ssize_t>(payload.size()), "write payload") && all_ok;

    rpc::SeekRequest sreq;
    sreq.set_fd(fd);
    sreq.set_offset(0);
    sreq.set_whence(SEEK_SET);
    rpc::SeekReply srep;
    brpc::Controller c10;
    stub.Seek(&c10, &sreq, &srep, nullptr);
    all_ok = expect(srep.offset() == 0, "seek begin") && all_ok;

    rpc::IORequestFD rreq;
    rreq.set_fd(fd);
    rreq.set_data(std::string(payload.size(), '\0')); // only size matters
    rpc::IOReplyFD rrep;
    brpc::Controller c11;
    stub.Read(&c11, &rreq, &rrep, nullptr);
    all_ok = expect(rrep.bytes() == static_cast<ssize_t>(payload.size()), "read payload") && all_ok;

    rpc::FdRequest fdreq;
    fdreq.set_fd(fd);
    rpc::Status stclose;
    brpc::Controller c12;
    stub.Close(&c12, &fdreq, &stclose, nullptr);
    all_ok = expect(stclose.code() == 0, "close fd") && all_ok;

    rpc::PathModeRequest keep_create;
    keep_create.set_path("/io/keep.bin");
    keep_create.set_mode(0644);
    brpc::Controller c13;
    stub.CreateFile(&c13, &keep_create, &st, nullptr);
    all_ok = expect(st.code() == 0, "create keep") && all_ok;

    rpc::OpenRequest keep_open;
    keep_open.set_path("/io/keep.bin");
    keep_open.set_flags(MO_RDWR);
    keep_open.set_mode(0644);
    rpc::IOReplyFD keep_open_reply;
    brpc::Controller c14;
    stub.Open(&c14, &keep_open, &keep_open_reply, nullptr);
    int keep_fd = static_cast<int>(keep_open_reply.bytes());
    all_ok = expect(keep_fd >= 0, "open keep") && all_ok;

    rpc::IORequestFD keep_wreq;
    keep_wreq.set_fd(keep_fd);
    keep_wreq.set_data("xyz");
    rpc::IOReplyFD keep_wrep;
    brpc::Controller c15;
    stub.Write(&c15, &keep_wreq, &keep_wrep, nullptr);
    all_ok = expect(keep_wrep.bytes() == 3, "write keep") && all_ok;

    rpc::PathRequest prem;
    prem.set_path("/io/keep.bin");
    rpc::Status strem;
    brpc::Controller c16;
    stub.RemoveFile(&c16, &prem, &strem, nullptr);
    all_ok = expect(strem.code() == 0, "remove keep") && all_ok;

    rpc::IORequestFD keep_rreq;
    keep_rreq.set_fd(keep_fd);
    keep_rreq.set_data(std::string(4, '\0'));
    rpc::IOReplyFD keep_rrep;
    brpc::Controller c17;
    stub.Read(&c17, &keep_rreq, &keep_rrep, nullptr);
    all_ok = expect(keep_rrep.bytes() == -1, "read after remove should fail") && all_ok;

    rpc::FdRequest fdreq2;
    fdreq2.set_fd(keep_fd);
    rpc::Status stc2;
    brpc::Controller c18;
    stub.Close(&c18, &fdreq2, &stc2, nullptr);
    all_ok = expect(stc2.code() != 0, "close already removed fd") && all_ok;

    rpc::PathModeRequest temp_create;
    temp_create.set_path("/io/temp.bin");
    temp_create.set_mode(0644);
    brpc::Controller c19;
    stub.CreateFile(&c19, &temp_create, &st, nullptr);
    all_ok = expect(st.code() == 0, "create temp") && all_ok;
    rpc::OpenRequest temp_open;
    temp_open.set_path("/io/temp.bin");
    temp_open.set_flags(MO_RDWR);
    temp_open.set_mode(0644);
    rpc::IOReplyFD temp_open_reply;
    brpc::Controller c20;
    stub.Open(&c20, &temp_open, &temp_open_reply, nullptr);
    int temp_fd = static_cast<int>(temp_open_reply.bytes());
    all_ok = expect(temp_fd >= 0, "open temp") && all_ok;
    rpc::FdRequest temp_sd;
    temp_sd.set_fd(temp_fd);
    rpc::Status stsd;
    brpc::Controller c21;
    stub.ShutdownFd(&c21, &temp_sd, &stsd, nullptr);
    all_ok = expect(stsd.code() == 0, "shutdown_fd") && all_ok;
    rpc::IORequestFD temp_rreq;
    temp_rreq.set_fd(temp_fd);
    temp_rreq.set_data(std::string(4, '\0'));
    rpc::IOReplyFD temp_rrep;
    brpc::Controller c22;
    stub.Read(&c22, &temp_rreq, &temp_rrep, nullptr);
    all_ok = expect(temp_rrep.bytes() == -1, "read after shutdown_fd") && all_ok;

    rpc::ColdInodeRequest cold_req;
    cold_req.set_max_candidates(10);
    cold_req.set_min_age_windows(1);
    rpc::ColdInodeListReply cold_list;
    brpc::Controller c23;
    stub.CollectColdInodes(&c23, &cold_req, &cold_list, nullptr);
    all_ok = expect(cold_list.inodes_size() <= 10, "collect_cold_inodes bound") && all_ok;
    rpc::ColdInodeBitmapRequest cold_breq;
    cold_breq.set_min_age_windows(1);
    rpc::ColdInodeBitmapReply cold_brep;
    brpc::Controller c24;
    stub.CollectColdInodesBitmap(&c24, &cold_breq, &cold_brep, nullptr);
    all_ok = expect(cold_brep.status().code() == 0, "cold bitmap exists") && all_ok;
    auto total_bits = cold_brep.bit_count();
    if (!expect(cold_brep.bitmap().size() >= total_bits, "bitmap sized")) return 27;
    rpc::ColdInodePercentRequest cold_preq;
    cold_preq.set_percent(50.0);
    rpc::ColdInodeListReply cold_prep;
    brpc::Controller c25;
    stub.CollectColdInodesByAtimePercent(&c25, &cold_preq, &cold_prep, nullptr);
    all_ok = expect(cold_prep.inodes_size() <= total_bits, "collect by percent") && all_ok;

    rpc::Status stshut;
    brpc::Controller c26;
    stub.Shutdown(&c26, &empty, &stshut, nullptr);
    all_ok = expect(stshut.code() == 0, "shutdown") && all_ok;

    if (all_ok) {
        std::cout << "VFS_new RPC integration test passed" << std::endl;
        return 0;
    }
    std::cout << "VFS_new RPC integration test finished with failures" << std::endl;
    return 1;
}
