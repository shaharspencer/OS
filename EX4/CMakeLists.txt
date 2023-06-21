cmake_minimum_required(VERSION 3.19)
project(OS)

set(CMAKE_CXX_STANDARD 11)

add_executable(test
#        MemoryConstants.h
        PhysicalMemory.h
        PhysicalMemory.cpp
        YaaraConstants.h
        YaaraTest.cpp
        VirtualMemory.h

        VirtualMemory.cpp
        )