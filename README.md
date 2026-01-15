grafana:
http://222.20.95.30:3000/
account:
admin
password:mysecurepassword
prometheus:
http://222.20.95.30:9090/

./msg/RPC/mds_rpc_server \
  --mds_port=8010 \
  --mds_data_dir=/mnt/md0/Projects/tmp_zb/mds \
  --mds_create_new=true \
  --node_alloc_policy=prefer_real \
  --log_file=/mnt/md0/Projects/ZBStorage/log/mds.log

./srm/srm_main \
  --srm_port=9100 \
  --mds_addr=127.0.0.1:8010 \
  --virtual_node_count=0 \
  --log_file=/mnt/md0/Projects/ZBStorage/log/srm.log

./storagenode/real_node/real_node_server \
  --port=9010 \
  --skip_mount=true \
  --base_path=/mnt/md0/Projects/tmp_zb/node_data \
  --srm_addr=127.0.0.1:9100 \
  --advertise_ip=127.0.0.1 \
  --log_file=/mnt/md0/Projects/ZBStorage/log/realnode.log

sudo ./client/fuse/zb_fuse_client \
  --mount_point=/mnt/md0/Projects/tmp_zb/mp_zb \
  --mds_addr=127.0.0.1:8010 \
  --srm_addr=127.0.0.1:9100 \
  --node_id=node-1 \
  --allow_other \
  --foreground \
  --log_file=/mnt/md0/Projects/ZBStorage/log/client.log

./test_fs_bench \
  --path=/mnt/md0/Projects/tmp_zb/mp_zb \
  --duration_sec=60 \
  --file_size_kb=1024 \
  --files=200 \
  --threads=4 \
  --read_pct=70 \
  --latency_samples=20000 \
  --log_file=/mnt/md0/Projects/ZBStorage/log/fs_bench.log

./test_fs_bench \
  --path=/mnt/md0/Projects/tmp_zb/loacl_zb \
  --duration_sec=60 \
  --file_size_kb=256 \
  --files=200 \
  --threads=4 \
  --read_pct=70 \
  --latency_samples=20000 \
  --log_file=/mnt/md0/Projects/ZBStorage/log/fs_bench.log

--path：挂载点目录（必填）
--duration_sec：测试时长（秒）
--file_size_kb：单文件大小（KB）
--files：测试文件数量
--threads：并发线程数
--read_pct：读比例（0-100），写比例=100-读
--fsync：每次写后调用 fsync（更真实但更慢）
--latency_samples：延迟采样数（用于 p50/p95/p99 估计）
--log_file：日志文件路径（追加写入）

./test_fs_verify \
  --path=/mnt/md0/Projects/tmp_zb/mp_zb \
  --files=10 \
  --file_size_kb=64 \
  --iterations=3 \
  --log_file=/mnt/md0/Projects/ZBStorage/log/fs_verify.log

--path：挂载点目录（必填）
--files：每轮测试文件数量
--file_size_kb：每个文件大小（KB）
--iterations：重复轮数
--fsync：写完后调用 fsync
--seed：生成数据的随机种子（便于复现）
--log_file：日志文件路径（追加写入）

下一步：
多个real node协同
增加virtual node
josn日志信息

fio --name=zbfs \
  --directory=/mnt/md0/Projects/tmp_zb/loacl_zb \
  --rw=randrw --rwmixread=70 \
  --bs=256k --size=256m --numjobs=2 --iodepth=1 \
  --ioengine=sync --direct=0 --time_based --runtime=60 \
  --group_reporting --fallocate=none

fio --name=zbfs \
  --directory=/mnt/md0/Projects/tmp_zb/mp_zb \
  --rw=randrw --rwmixread=70 \
  --bs=256k --size=256m --numjobs=2 --iodepth=1 \
  --ioengine=sync --direct=0 --time_based --runtime=60 \
  --group_reporting --fallocate=none


./tests/build/test_inode_capacity_sim \
  --inode_dir /mnt/md0/inodeNS \
  --json_log /mnt/md0/Projects/ZBStorage/log/inode_capacity_log.json \
  --ssd_nodes 1001 --hdd_nodes 1001 --mix_nodes 1001 \
  --ssd_devices_per_node 1 --hdd_devices_per_node 1 \
  --ssd_capacity_bytes $((350*1024*1024*1024*1024)) \
  --hdd_capacity_bytes $((50*1024*1024*1024*1024)) \
  --max_inodes_per_sec 50000 \
  --report_interval_sec 3 \
  --start_file inode_chunk_0.bin \
  --start_index 0

  ./tests/build/test_inode_dump --file /mnt/md0/inodeNS/inode_chunk_0.bin --count 5 --offset 0

