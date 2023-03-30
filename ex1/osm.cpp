#include <sys/time.h>
#include <iostream>
#include "osm.h"

#define LOOP_UNROLLING 10
#define ITERATIONS 10000000
#define ERROR_CODE -1

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
    return ERROR_CODE;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    unsigned int j = 0;
    int ok = gettimeofday (&start_time, NULL);
    if (ok)
    {
      return ERROR_CODE;
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
    if (ok)
    {
      return ERROR_CODE;
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
    return ERROR_CODE;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    int ok = gettimeofday (&start_time, NULL);
    if (ok)
    {
      return ERROR_CODE;
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
    if (ok)
    {
      return ERROR_CODE;
    }
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

double osm_syscall_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return ERROR_CODE;
  }
  unsigned int epochs = iterations / LOOP_UNROLLING;
  timeval start_time, end_time;
  double time_diff = 0;
  for (unsigned int i = 0; i < epochs; i++)
  {
    int ok = gettimeofday (&start_time, NULL);
    if (ok)
    {
      return ERROR_CODE;
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
    if (ok)
    {
      return ERROR_CODE;
    }
    time_diff += calculate_time_diff (&start_time, &end_time);
  }
  return time_diff / (double) iterations;
}

void run_file (unsigned int num_iterations)
{
  std::cout << "Iterations: " << num_iterations << std::endl;
  double time = osm_operation_time (num_iterations);
  if (time == ERROR_CODE)
  {
      std::cout << "Error" << std::endl;
      return;
  }
  std::cout << "Arithmetic Operation Time: " << time << std::endl;
  time = osm_function_time (num_iterations);
  if (time == ERROR_CODE)
  {
    std::cout << "Error" << std::endl;
    return;
  }
  std::cout << "Function Call Time: " << time << std::endl;
  time = osm_syscall_time (num_iterations);
  if (time == ERROR_CODE)
  {
    std::cout << "Error" << std::endl;
    return;
  }
  std::cout << "System Call Time: " << time << std::endl;
}

/*
int main ()
{
  run_file (ITERATIONS);
  run_file (2 * ITERATIONS);
  return 0;
}
*/