CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(kv_store)

#SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lpthread ")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSLOW_DEBUG -Wall -pipe -O3 -march=native -fPIC -fomit-frame-pointer -fno-exceptions -fno-rtti ")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSLOW_DEBUG -Wall -pipe -O3 -march=native -fPIC -fomit-frame-pointer -fno-exceptions ")
SET(SRC_BASE ${PROJECT_SOURCE_DIR})
SET(SRC_CPP ${SRC_BASE}/kv_store.cc
    )

ADD_LIBRARY(${PROJECT_NAME} SHARED ${SRC_CPP})
TARGET_LINK_LIBRARIES(${PROJECT_NAME} pthread)

