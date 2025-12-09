#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "../inode/inode.h"

class MdsServer;
class ImageManager;
namespace srm {
using ImageManager = ::ImageManager;
}

struct ColdScanRange {
	uint64_t start_ino = 0;  // 起始 inode 号（包含）
	uint64_t end_ino = 0;    // 结束 inode 号（包含），0 代表追踪到最大
};

struct ColdScanResult {
	std::vector<uint64_t> cold_inodes;  // 本轮识别出的冷 inode 集合
	std::vector<Inode> inode_records;   // 对应的 inode 元数据（已反序列化）
	std::chrono::system_clock::time_point collected_at;  // 扫描时间戳
};

struct ColdCollectorConfig {
	ColdScanRange scan_range{0, 0};              // 限定扫描的 inode 区间
	std::chrono::hours scan_interval{24};        // 后台线程每次扫描的间隔
	std::chrono::hours cold_threshold{24 * 180}; // “冷”判定阈值：距离最近访问时间超过此值即视为冷
	size_t max_inodes_per_round = 50'000;        // 单个扫描周期内最多检查多少个 inode
	size_t max_batch_size = 10'000;              // 传递给镜像聚合的一次批处理最大数量
	std::chrono::seconds delay_before_burn{300}; // 聚合完成后等待多久再发刻录请求
	std::string inode_directory = "/mnt/md0/inode"; // 批量生成的 inode 文件所在目录
	uint64_t image_flush_threshold_bytes = 10ULL * 1024 * 1024 * 1024; // 累计文件大小达到该值触发镜像封装
};

class IColdInodeSelector {
public:
	virtual ~IColdInodeSelector() = default;
	// 返回 true 代表根据配置判断 inode 为冷数据。
	virtual bool is_cold(const Inode& inode, const ColdCollectorConfig& cfg) const = 0;
};

class IImageAggregationScheduler {
public:
	virtual ~IImageAggregationScheduler() = default;
	// 将扫描结果交给镜像聚合调度器，可能触发多个异步任务。
	virtual void schedule_aggregation(const ColdScanResult& result) = 0;
};

class ColdDataCollectorService {
public:
	// @param mds MDS 服务实例，用于读取 inode / 时间等信息。
	// @param image_mgr SRM 镜像管理器，用于聚合/刻录。
	// @param cfg 初始配置（可后续动态更新）。
	ColdDataCollectorService(MdsServer* mds,
						 srm::ImageManager* image_mgr,
						 ColdCollectorConfig cfg);
	~ColdDataCollectorService();

	// 启动后台线程（幂等）。
	void start();
	// 请求停止线程并阻塞等待退出。
	void stop();
	// 动态更新配置，例如扫描区间、阈值等。
	void update_config(const ColdCollectorConfig& cfg);

	// 注入自定义冷数据判定策略。
	void set_selector(std::shared_ptr<IColdInodeSelector> selector);
	// 注入自定义镜像调度策略（默认直接调用 ImageManager）。
	void set_scheduler(std::shared_ptr<IImageAggregationScheduler> scheduler);

	// 仅用于测试/排障：立即执行一次扫描并返回结果（不会提交至 ImageManager）。
	ColdScanResult run_single_scan_for_test();

private:
	void run_loop();                                    // 线程入口
	ColdScanResult scan_once();                         // 执行一次扫描并返回结果
	void buffer_pending_inodes(const ColdScanResult& result);   // 累积待封装的 inode 数据
	void flush_pending_if_needed(bool force);           // 满足阈值或强制时触发封装
	void submit_to_image_manager(const ColdScanResult& result); // 调用 ImageManager 进行聚合
	void queue_burn_request(const ColdScanResult& result);      // 根据结果排程刻录
	ColdCollectorConfig snapshot_config() const;        // 读取当前配置的线程安全副本

	MdsServer* mds_;
	srm::ImageManager* image_mgr_;
	std::shared_ptr<IColdInodeSelector> selector_;
	std::shared_ptr<IImageAggregationScheduler> scheduler_;
	mutable std::mutex hook_mtx_;

	ColdCollectorConfig config_;
	std::thread worker_;
	mutable std::mutex config_mtx_;
	std::atomic<bool> running_{false};

	std::vector<Inode> pending_inodes_;
	uint64_t pending_bytes_ = 0;
};
