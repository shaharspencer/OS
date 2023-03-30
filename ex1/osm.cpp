#include <sys/time.h>
#include <iostream>
#include "osm.h"

#define LOOP_UNROLLING 10
#define ITERATIONS 10000000

double calculate_time_diff (timeval* start_time, timeval* end_time)
{
  double start = (double)(start_time->tv_sec) * (1e6) + (double)(start_time->tv_usec);
  double end = (double)(end_time->tv_sec) * (1e6) + (double)(end_time->tv_usec);
  return 1e3 * (end - start);
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
    unsigned int j = 0;
    int ok = gettimeofday (&start_time, NULL);
    if (!ok)
    {
      return -1;
    }
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    j += 1;
    ok = gettimeofday (&end_time, NULL);
    if (!ok)
    {
      return -1;
    }
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
    int ok = gettimeofday (&start_time, NULL);
    if (!ok)
    {
      return -1;
    }
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
    ok = gettimeofday (&end_time, NULL);
    if (!ok)
    {
      return -1;
    }
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
    int ok = gettimeofday (&start_time, NULL);
    if (!ok)
    {
      return -1;
    }
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
    ok = gettimeofday (&end_time, NULL);
    if (!ok)
    {
      return -1;
    }
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

void run_file (unsigned int num_iterations)
{
  std::cout << "Iterations: " << num_iterations << std::endl;
  std::cout << "Arithmetic Operation Time: " << osm_operation_time (num_iterations) << std::endl;
  std::cout << "Function Call Time: " << osm_function_time (num_iterations) << std::endl;
  std::cout << "System Call Time: " << osm_syscall_time (num_iterations) << std::endl;
}

int main ()
{
  run_file (ITERATIONS);
  run_file (2 * ITERATIONS);
  return 0;
}