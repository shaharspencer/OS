cmake_minimum_required(VERSION 3.10)
project(OS_ex3)

set(CMAKE_CXX_STANDARD 11)

# Find pthread library
find_package(Threads REQUIRED)
if (THREADS_FOUND)
    message(STATUS "Found Threads: ${CMAKE_THREAD_LIBS_INIT}")
else()
    message(FATAL_ERROR "Threads library not found.")
endif()


# Add executable and include directories
add_executable(test
        Barrier/Barrier.h Barrier/Barrier.cpp
        Sample_Client/SampleClient.cpp MapReduceFramework.cpp
        MapReduceFramework.h MapReduceClient.h)

# Link required libraries
target_link_libraries(test PRIVATE ${CMAKE_THREAD_LIBS_INIT})
