cmake_minimum_required(VERSION 3.18.4)
project(nimbridge LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(nimbridge SHARED nim_bridge.cpp)

add_library(nimlibp2p SHARED IMPORTED)
set_target_properties(
  nimlibp2p
  PROPERTIES
    IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/../libs/arm64-v8a/libnimlibp2p.so"
)

target_include_directories(
  nimbridge
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  nimbridge
  PUBLIC
    nimlibp2p
    hilog_ndk.z
    ssl
    crypto
)
