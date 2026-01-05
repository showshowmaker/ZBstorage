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
  --mds_thread_num=4

./srm/srm_main \
  --srm_port=9100 \
  --mds_addr=127.0.0.1:8010 \
  --virtual_node_count=2 \
  --virtual_node_capacity_bytes=$((50*1024*1024*1024)) \
  --virtual_min_latency_ms=5 --virtual_max_latency_ms=20 \
  --virtual_failure_rate=0.0

./storagenode/real_node/real_node_server \
  --port=9010 \
  --skip_mount=true \
  --base_path=/mnt/md0/Projects/tmp_zb/node_data \
  --srm_addr=127.0.0.1:9100 \
  --advertise_ip=127.0.0.1 \
  --agent_heartbeat_ms=3000 \
  --agent_register_backoff_ms=5000 \
  --sync_on_write=false

sudo ./client/fuse/zb_fuse_client \
  --mount_point=/mnt/md0/Projects/mp_zb \
  --mds_addr=127.0.0.1:8010 \
  --vfs_addr=127.0.0.1:8012



//没用的
./msg/RPC/storage_rpc_server \
  --storage_port=8011 \
  --storage_thread_num=4 \
  --storage_idle_timeout=-1

./msg/RPC/vfs_rpc_server \
  --vfs_port=8012 \
  --mds_addr=127.0.0.1:8010 \
  --storage_addr=127.0.0.1:8011 \
  --vfs_thread_num=4

