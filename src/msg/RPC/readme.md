cmake --build build/rpc -j

rm -rf /mnt/md0/Projects/tmp_dir_store
mkdir -p /mnt/md0/Projects/tmp_dir_store/dir_store

build/rpc/storage_rpc_server --storage_port=8011
build/rpc/mds_rpc_server --mds_port=8010 --mds_data_dir=/mnt/md0/Projects/tmp_dir_store --mds_create_new=true
build/rpc/vfs_rpc_server --vfs_port=8012 --mds_addr=127.0.0.1:8010 --storage_addr=127.0.0.1:8011
build/rpc/rpc_client --vfs_addr=127.0.0.1:8012