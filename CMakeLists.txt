cmake_minimum_required(VERSION 2.8.4)
project(BwTree)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp bwtree)
add_executable(BwTree ${SOURCE_FILES})