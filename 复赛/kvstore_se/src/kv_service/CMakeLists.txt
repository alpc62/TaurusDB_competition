cmake_minimum_required(VERSION 2.8)
project(kv_service)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pipe -O3 -march=native -fPIC -fomit-frame-pointer -fno-exceptions ")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
    ${SRC_BASE}/../include
    )

link_directories(
    )

set(SRC_SERVICE
    ${SRC_BASE}/kv_service.cc
    )

add_library(${PROJECT_NAME} SHARED ${SRC_SERVICE})
TARGET_LINK_LIBRARIES(${PROJECT_NAME} pthread)
