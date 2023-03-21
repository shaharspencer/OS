cmake_minimum_required(VERSION 3.16)
project(OS)

set(CMAKE_CXX_STANDARD 14)

include_directories(ex1)

add_executable(OS
        ex1/osm.cpp
        ex1/osm.h)
