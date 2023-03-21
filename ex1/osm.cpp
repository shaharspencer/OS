#include <osm.h>
#include <sys/time.h>

double calculate_normalized_time (unsigned int iterations, timeval* start_time, timeval* end_time) {
  suseconds_t start = (start_time->tv_sec * 10^9) + (start_time->tv_usec * 10^3);
  suseconds_t end = (end_time->tv_sec * 10^9) + (end_time->tv_usec * 10^3);
  return (double)((end - start) / iterations);  // TODO: figure out WTF
}

double osm_operation_time (unsigned int iterations) {
  if (iterations == 0) {
      return -1;
  }
  auto *start_time = new struct timeval;
  auto *end_time = new struct timeval;
  if (gettimeofday (start_time, nullptr) == -1) {
    // exception
  }
  for (unsigned int i = 0; i < iterations; i++) {
    1 + 1;
  }
  if (gettimeofday (end_time, nullptr) == -1) {
    // exception
  }
  return calculate_normalized_time (iterations, start_time, end_time);
}

void null_function() {}

double osm_function_time (unsigned int iterations) {
  if (iterations == 0) {
      return -1;
    }
  auto *start_time = new struct timeval;
  auto *end_time = new struct timeval;
  if (gettimeofday (start_time, nullptr) == -1) {
      // exception
    }
  for (unsigned int i = 0; i < iterations; i++) {
      null_function ();
    }
  if (gettimeofday (end_time, nullptr) == -1) {
      // exception
    }
  return calculate_normalized_time (iterations, start_time, end_time);
}

double osm_syscall_time (unsigned int iterations) {
  if (iterations == 0) {
      return -1;
    }
  auto *start_time = new struct timeval;
  auto *end_time = new struct timeval;
  if (gettimeofday (start_time, nullptr) == -1) {
      // exception
    }
  for (unsigned int i = 0; i < iterations; i++) {
      OSM_NULLSYSCALL;
    }
  if (gettimeofday (end_time, nullptr) == -1) {
      // exception
    }
  return calculate_normalized_time (iterations, start_time, end_time);
}