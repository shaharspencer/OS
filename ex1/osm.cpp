#include <sys/time.h>
#include <iostream>
#include "osm.h"

#define LOOP_UNROLLING 10
#define HUNDRED_K 100000

double calculate_time_diff (timeval *start_time, timeval *end_time)
{
  double start =
      (double) ((start_time->tv_sec * 10 ^ 9) +
                (start_time->tv_usec * 10 ^ 3));
  double end =
      (double) ((end_time->tv_sec * 10 ^ 9) +
                (end_time->tv_usec * 10 ^ 3));
  return end - start;
}

double osm_operation_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    gettimeofday (&start_time, nullptr);
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    1 + 1;
    gettimeofday (&end_time, nullptr);
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

void null_function ()
{}

double osm_function_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    gettimeofday (&start_time, nullptr);
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    null_function ();
    gettimeofday (&end_time, nullptr);
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

double osm_syscall_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    gettimeofday (&start_time, nullptr);
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    gettimeofday (&end_time, nullptr);
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

void run_file (unsigned int num_iterations)
{
  std::cout << "Iterations: " << num_iterations << std::endl;
  std::cout << "arithmetic result: ";
  std::cout << osm_operation_time (num_iterations) << std::endl;
  std::cout << "null function result: ";
  std::cout << osm_function_time (num_iterations) << std::endl;
  std::cout << "syscall result: ";
  std::cout << osm_syscall_time (num_iterations) << std::endl;
}

int main ()
{
  run_file (HUNDRED_K);
  run_file (2 * HUNDRED_K);
  return 0;
}