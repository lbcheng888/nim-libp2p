cmake_minimum_required(VERSION 3.22.1)
project(nimbridge LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(nimbridge SHARED nim_bridge.cpp)

target_include_directories(nimbridge PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_directories(nimbridge PRIVATE "${CMAKE_SOURCE_DIR}/../jniLibs/${ANDROID_ABI}")

target_link_libraries(
  nimbridge
  PRIVATE
    nimlibp2p
    log
    android
)
