#pragma once
#include <cstdint>
#include "../../debug/ZBLog.h"
#include <climits>
#include <string>
#include <vector>
#include <iostream>
#include <ctime>
#include <array>
#include <bitset>
#include <unordered_map>
#include <memory>
#include <cstring>
#include <mutex>
#include <map>
#include "../../fs/volume/Volume.h"
#include "../../msg/IO.h"
#include "../../storagenode/StorageNode.h"
#include "../../storagenode/StorageTypes.h"
#include <boost/dynamic_bitset.hpp>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <random>
#include <stdexcept>
#include "InodeTimestamp.h"

// 增强的文件类型宏定义（匹配POSIX）
#define ZB_S_IFMT   0170000  // 文件类型掩码
#define ZB_S_IFREG  0100000  // 普通文件
#define ZB_S_IFDIR  0040000  // 目录
#define ZB_S_IFLNK  0120000  // 符号链接
#define ZB_S_IFBLK  0060000  // 块设备
#define ZB_S_IFCHR  0020000  // 字符设备
#define ZB_S_IFIFO  0010000  // 管道
#define ZB_S_IFSOCK 0140000  // 套接字

// 文件访问权限宏定义
#define ZB_S_IRWXU  00700    // 所有者读写执行权限
#define ZB_S_IRUSR  00400    // 所有者读权限
#define ZB_S_IWUSR  00200    // 所有者写权限
#define ZB_S_IXUSR  00100    // 所有者执行权限
#define ZB_S_IRWXG  00070    // 组读写执行权限
#define ZB_S_IRGRP  00040    // 组读权限
#define ZB_S_IWGRP  00020    // 组写权限
#define ZB_S_IXGRP  00010    // 组执行权限
#define ZB_S_IRWXO  00007    // 其他用户读写执行权限
#define ZB_S_IROTH  00004    // 其他用户读权限
#define ZB_S_IWOTH  00002    // 其他用户写权限
#define ZB_S_IXOTH  00001    // 其他用户执行权限

// 特殊权限位
#define ZB_S_ISUID  04000    // set-user-ID
#define ZB_S_ISGID  02000    // set-group-ID
#define ZB_S_ISVTX  01000    // 粘滞位

// POSIX兼容性常量
constexpr size_t ZB_NAME_MAX = 255;     // 最大文件名长度
constexpr size_t ZB_PATH_MAX = 4096;    // 最大路径长度
constexpr int MO_RDONLY = 0x0000;     // 只读模式
constexpr int MO_WRONLY = 0x0001;     // 只写模式
constexpr int MO_RDWR = 0x0002;       // 读写模式
constexpr int MO_CREAT = 0x0100;      // 创建标志
constexpr int MO_TRUNC = 0x0200;      // 截断标志
constexpr int MO_APPEND = 0x0400;     // 追加标志

// 文件类型枚举
enum class FileType {
    Unknown = 0,    // DT_UNKNOWN   未知类型
    Regular = 1,    // DT_REG       普通文件
    Directory = 2,  // DT_DIR       目录
    Symlink = 3,    // DT_LNK       符号链接
    BlockDev = 4,   // DT_BLK       块设备
    CharDev = 5,    // DT_CHR       字符设备
    Fifo = 6,       // DT_FIFO      管道
    Socket = 7      // DT_SOCK      套接字
};

//元数据类
class Inode {
public:
    // 位域编码位置：14bit 节点编号 + 2bit 类型
    union{
        struct{
            uint16_t node_id : 14;   // 0-16383 的存储节点编号
            uint16_t node_type : 2;  // 00 SSD, 01 HDD, 10 混合, 11 保留
        }fields;
        uint16_t raw;                // 序列化时使用的紧凑表示
    }location_id;

    // 16比特设备块号
    uint16_t block_id;

    // 8比特文件名长度，8比特摘要长度
    uint8_t filename_len;
    uint8_t digest_len;

    // 变长文件名和摘要
    std::string filename;
    std::vector<uint8_t> digest;

    // 要知道这个inode是目录文件还是普通文件
    union{
        struct {
            uint16_t file_type : 4;
            uint16_t file_perm : 12; // 12比特权限位，4比特权限
        }fields;
        uint16_t raw;
    }file_mode;

    // 2比特文件大小单位，14比特文件大小
    // 文件大小单位指的是byte KB MB GB
    union{
        struct{
            uint16_t size_unit : 2;
            uint16_t file_size : 14;
        }fields;
        uint16_t raw;
    }file_size;

    // @wbl Inode 号
    uint64_t inode; // 64比特inode号

    // 命名空间ID，固定长度32字节（ASCII）
    std::string namespace_id;

    // 四个时间戳
    InodeTimestamp fm_time;   // 文件最后修改时间 file modification time
    InodeTimestamp fa_time;   // 文件最后访问时间 file access time
    InodeTimestamp im_time;   // 元数据最后变更时间 inode  modification time
    InodeTimestamp fc_time;  // 文件创建时间 file creation time

    // 卷id
    std::string volume_id; // 卷id，存储卷的唯一标识符

    // 块分配映射 demo
    // std::vector<size_t> block_ids; // 分配的所有块号列表 -->> 修改为block_segement结构
    std::vector<BlockSegment> block_segments;

    Inode();

    /**
     * @brief 设置存储节点 ID。
     * @param id 节点标识。
     */
    void setNodeId(uint16_t id);

    /**
     * @brief 设置存储节点类型。
     * @param type 0:SSD 1:HDD 2:混合 3:保留。
     */
    void setNodeType(uint8_t type);

    /**
     * @brief 设置块 ID。
     * @param id 块标识。
     */
    void setBlockId(uint16_t id);

    /**
     * @brief 设置命名空间 ID
     * @param id 命名空间标识
     */
    void setNamespaceId(const std::string& id);

    /**
     * @brief 获取命名空间 ID
     * @return 命名空间标识
     */
    const std::string& getNamespaceId() const;

    static constexpr size_t kNamespaceIdLen = 32;

    /**
     * @brief 设置文件名。
     * @param name 文件名字符串。
     */
    void setFilename(const std::string& name);

    /**
     * @brief 设置摘要数据。
     * @param dig 摘要字节序列。
     */
    void setDigest(const std::vector<uint8_t>& dig);

    /**
     * @brief 设置文件类型。
     * @param type 类型常量。
     */
    void setFileType(uint8_t type);

    /**
     * @brief 设置权限掩码。
     * @param perm 权限位。
     */
    void setFilePerm(uint16_t perm);

    /**
     * @brief 设置文件大小单位。
     * @param unit 0:byte 1:KB 2:MB 3:GB。
     */
    void setSizeUnit(uint16_t unit);

    /**
     * @brief 设置文件大小字段值。
     * @param size 14 位大小值。
     */
    void setFileSize(uint16_t size);

    /**
     * @brief 设置修改时间。
     * @param t 时间戳。
     */
    void setFmTime(const InodeTimestamp& t);

    /**
     * @brief 设置访问时间。
     * @param t 时间戳。
     */
    void setFaTime(const InodeTimestamp& t);

    /**
     * @brief 设置创建时间。
     * @param t 时间戳。
     */
    void setFcTime(const InodeTimestamp& t);

    /**
     * @brief 设置卷 ID。
     * @param id 卷 UUID。
     */
    void setVolumeId(const std::string& id);

    /**
     * @brief 获取卷 UUID。
     * @return 卷 UUID 字符串引用。
     */
    const std::string& getVolumeUUID() const;

    /**
     * @brief 以字节为单位返回文件大小。
     * @return 文件大小。
     */
    uint64_t getFileSize() const;

    /**
     * @brief 获取块段列表。
     * @return 块段常量引用。
     */
    const std::vector<BlockSegment>& getBlocks() const;

    /**
     * @brief 追加块段。
     * @param segments 块段数组。
     */
    void appendBlocks(const std::vector<BlockSegment>& segments);

    /**
     * @brief 清空块段列表。
     */
    void clearBlocks();

    /**
     * @brief 返回占用块数。
     * @return 块计数。
     */
    size_t block_count() const;

    /**
     * @brief 查找逻辑块对应物理块。
     * @param logical_block 逻辑块号。
     * @param physical_block 输出物理块号。
     * @return 找到返回 true。
     */
    bool find_physical_block(size_t logical_block, size_t& physical_block) const;

    /**
     * @brief 序列化为字节数组。
     * @return 序列化数据。
     */
    std::vector<uint8_t> serialize() const;

    /**
     * @brief 反序列化 inode。
     * @param data 源字节流。
     * @param offset 当前偏移，解析后会更新。
     * @param out_inode 输出 inode。
     * @param total_size 数据总长度。
     * @return 成功返回 true。
     */
    static bool deserialize(const uint8_t* data, size_t& offset, Inode& out_inode, size_t total_size);
};
