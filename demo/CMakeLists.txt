cmake_minimum_required(VERSION 3.10)
project(zb_system_demo CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(GFlags REQUIRED)
find_package(Threads REQUIRED)

set(PROJECT_ROOT ${CMAKE_CURRENT_LIST_DIR}/..)

add_executable(zb_system_test
  zb_system_test.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeStorage.cpp
  ${PROJECT_ROOT}/src/mds/inode/inode.cpp
  ${PROJECT_ROOT}/src/mds/inode/InodeTimestamp.cpp
)

target_include_directories(zb_system_test PRIVATE
  ${PROJECT_ROOT}/src
  ${PROJECT_ROOT}/src/mds
  ${PROJECT_ROOT}/src/debug
  ${PROJECT_ROOT}/third_party
)

target_link_libraries(zb_system_test PRIVATE
  ${GFLAGS_LIBRARIES}
  Threads::Threads
)