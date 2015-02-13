cmake_minimum_required(VERSION 2.8.4)
project(BwTree)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Werror -pthread -lpthread -std=c++11")

set(SOURCE_FILES main.cpp main.hpp bwtree.hpp bwtree.cpp epoque.hpp nodes.hpp)

find_package (Threads)
add_executable(BwTree ${SOURCE_FILES})

target_link_libraries (BwTree ${CMAKE_THREAD_LIBS_INIT})
