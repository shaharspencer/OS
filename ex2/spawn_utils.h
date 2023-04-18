#ifndef UTHREADS_H_SPAWN_UTILS_H
#define UTHREADS_H_SPAWN_UTILS_H

/**
 * Each existing thread has a unique thread ID, which is a non-negative integer.
 * The ID given to a new thread must be the smallest non-negative integer not already taken by an
 * existing thread (i.e. if a thread with ID 1 is terminated and then a new thread is spawned, it
 * should receive 1 as its ID). The maximal number of threads the library should support (including
 * the main thread) is MAX_THREAD_NUM.
 * @return valid ID if there's a free one, else -1
 */
int decide_tid ();

#endif //UTHREADS_H_SPAWN_UTILS_H
