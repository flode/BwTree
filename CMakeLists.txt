cmake_minimum_required(VERSION 2.8.4)
project(BwTree)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Werror -pthread -std=c++14")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror")

find_package (Threads)
set(SOURCE_FILES bwtree.hpp bwtree.cpp epoque.hpp nodes.hpp)
add_library(BwTreeLib ${SOURCE_FILES})
target_link_libraries (BwTreeLib ${CMAKE_THREAD_LIBS_INIT})

add_executable(BwTree main.cpp main.hpp)
