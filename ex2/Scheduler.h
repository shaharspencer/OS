#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include "Thread.h"

#include <signal.h>
#include <setjmp.h>
#include <sys/time.h>
#include <system_error>
#include <stdexcept>
#include <system_error>

#include <deque>
#include <set>
#include <iostream>

#define FAILURE (-1)
#define SUCCESS 0
#define MAIN_TID 0
#define PREEMPTED 100

class Scheduler {
private:
    static Scheduler* instance;

    /* Timer components */
    const suseconds_t quantum;
    struct itimerval timer;
    int total_quanta_counter;
    void increment_total_quanta_counter() { total_quanta_counter++; }

    /* Threads components */
    Thread* threads[MAX_THREAD_NUM];
    int get_free_tid();
    bool is_tid_valid(int tid);
    int running_thread;
    void schedule();
    void timer_handler(int sig);
    std::deque<int> *ready_threads;
    void remove_from_ready(int tid);
    std::set<int> *blocked_threads;
    std::set<int> *sleeping_threads;
    void handle_sleeping_threads();

    /* Signals component */
    sigset_t signals;
    void sigprocmask_block();
    void sigprocmask_unblock();

    /* A method to exit the Scheduler's run and dealloc all data */
    void error_handler(std::string error_type, std::string description, std::string context);

public:
    /**
     * @brief initializes the thread library.
     *
     * Once this function returns, the main thread (tid == 0) will be set as RUNNING. There is no need to
     * provide an entry_point or to create a stack for the main thread - it will be using the "regular" stack and PC.
     * You may assume that this function is called before any other thread library function, and that it is called
     * exactly once.
     * The input to the function is the length of a quantum in micro-seconds.
     * It is an error to call this function with non-positive quantum_usecs.
     */
    explicit Scheduler(int quantum_usecs);
    ~Scheduler();

    static void static_timer_handler(int sig) { instance->timer_handler(sig); }

    /**
     * @brief Creates a new thread, whose entry point is the function entry_point with the signature
     * void entry_point(void).
     *
     * The thread is added to the end of the READY threads list.
     * The uthread_spawn function should fail if it would cause the number of concurrent threads to exceed the
     * limit (MAX_THREAD_NUM).
     * Each thread should be allocated with a stack of size STACK_SIZE bytes.
     * It is an error to call this function with a null entry_point.
     *
     * @return On success, return the ID of the created thread. On failure, return -1.
     */
    int spawn(thread_entry_point entry_point);

    /**
     * @brief Terminates the thread with ID tid and deletes it from all relevant control structures.
     *
     * All the resources allocated by the library for this thread should be released. If no thread with ID tid exists it
     * is considered an error. Terminating the main thread (tid == 0) will result in the termination of the entire
     * process using exit(0) (after releasing the assigned library memory).
     *
     * @return The function returns 0 if the thread was successfully terminated and -1 otherwise. If a thread terminates
     * itself or the main thread is terminated, the function does not return.
     */
    int terminate(int tid);

    /**
     * @brief Blocks the thread with ID tid. The thread may be resumed later using uthread_resume.
     *
     * If no thread with ID tid exists it is considered as an error. In addition, it is an error to try blocking the
     * main thread (tid == 0). If a thread blocks itself, a scheduling decision should be made. Blocking a thread in
     * BLOCKED state has no effect and is not considered an error.
     *
     * @return On success, return 0. On failure, return -1.
     */
    int block(int tid);

    /**
     * @brief Resumes a blocked thread with ID tid and moves it to the READY state.
     *
     * Resuming a thread in a RUNNING or READY state has no effect and is not considered as an error. If no thread with
     * ID tid exists it is considered an error.
     *
     * @return On success, return 0. On failure, return -1.
     */
    int resume(int tid);

    /**
     * @brief Blocks the RUNNING thread for num_quantums quantums.
     *
     * Immediately after the RUNNING thread transitions to the BLOCKED state a scheduling decision should be made.
     * After the sleeping time is over, the thread should go back to the end of the READY queue.
     * If the thread which was just RUNNING should also be added to the READY queue, or if multiple threads wake up
     * at the same time, the order in which they're added to the end of the READY queue doesn't matter.
     * The number of quantums refers to the number of times a new quantum starts, regardless of the reason. Specifically,
     * the quantum of the thread which has made the call to uthread_sleep isn’t counted.
     * It is considered an error if the main thread (tid == 0) calls this function.
     *
     * @return On success, return 0. On failure, return -1.
     */
    int sleep(int num_quanta);

    /**
     * @brief Returns the thread ID of the calling thread.
     *
     * @return The ID of the calling thread.
     */
    int get_running_thread() { return running_thread; }

    /**
     * @brief Returns the total number of quantums since the library was
     * initialized, including the current quantum.
     *
     * Right after the call to uthread_init, the value should be 1.
     * Each time a new quantum starts, regardless of the reason, this number should be increased by 1.
     *
     * @return The total number of quantums.
     */
    int get_total_quanta_counter() { return total_quanta_counter; }

    /**
     * @brief Returns the number of quantums the thread with ID tid was in RUNNING state.
     *
     * On the first time a thread runs, the function should return 1. Every additional quantum that the thread starts should
     * increase this value by 1 (so if the thread with ID tid is in RUNNING state when this function is called, include
     * also the current quantum). If no thread with ID tid exists it is considered an error.
     *
     * @return On success, return the number of quantums of the thread with ID tid. On failure, return -1.
     */
    int get_quanta_counter(int tid);

    int previous_thread;
};


#endif //UTHREADS_H_SCHEDULER_H
