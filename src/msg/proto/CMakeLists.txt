cmake_minimum_required(VERSION 3.10)

find_package(Protobuf REQUIRED)

set(PROTO_SRC
  ${CMAKE_SOURCE_DIR}/msg/RPC/proto/rpc_common.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/storage_node.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/cluster_manager.proto
)

# Allow imports like "RPC/proto/..." used inside the proto.
set(PROTOBUF_IMPORT_DIRS
  ${CMAKE_SOURCE_DIR}/msg
  ${CMAKE_SOURCE_DIR}/msg/RPC/proto
)
protobuf_generate_cpp(STORAGENODE_PROTO_SRCS STORAGENODE_PROTO_HDRS ${PROTO_SRC})

add_library(storagenode_proto ${STORAGENODE_PROTO_SRCS} ${STORAGENODE_PROTO_HDRS})
target_include_directories(storagenode_proto
  PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/msg
)
target_link_libraries(storagenode_proto
  PUBLIC
    protobuf::libprotobuf
)

set_target_properties(storagenode_proto PROPERTIES
  OUTPUT_NAME storagenode_proto
  POSITION_INDEPENDENT_CODE ON
)
