cmake_minimum_required(VERSION 2.8)
project(kv_store)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -pipe -O3 -march=native -fPIC -fomit-frame-pointer -fno-exceptions ")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pipe -O3 -march=native -fPIC -fomit-frame-pointer -fno-exceptions -fno-rtti ")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
    ${SRC_BASE}/../include
    )

link_directories(
    )

set(SRC_KVSTORE
    ${SRC_BASE}/kv_store.c
    )

add_executable(${PROJECT_NAME} ${SRC_KVSTORE})
target_link_libraries(${PROJECT_NAME} pthread)

