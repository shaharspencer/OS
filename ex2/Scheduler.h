#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include "Thread.h"
#include <deque>
#include <set>
#include <sys/time.h>
#include <signal.h>
#include <error.h>

#define FAILURE (-1)
#define SUCCESS 0
#define MAIN_TID 0
#define RUNNING_THREAD_TERMINATED 100

class Scheduler {
private:
    /* Timer components */
    const suseconds_t quantum;
    struct itimer timer;
    int total_quanta_counter;

    /* Threads components */
    Thread* threads[MAX_THREAD_NUM];
    int running_thread;
    deque<int> *ready_threads;
    set<int> *blocked_threads;

    /* Signals component */
    sigset_t signals;

    int get_free_tid();
    bool is_tid_valid(int tid);

    void remove_from_ready(int tid);

    /**
     * Install timer_handler as the signal handler for SIGVTALRM.
     * if this function is called, move thread to the end of the queue.
     * @return
     */
    bool install_timer_handler();

    void increment_total_quanta_counter() { total_quanta_counter++; }

public:
    Scheduler(int quantum_usecs);
    ~Scheduler();

    bool init();

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

    int sleep(int tid);

    /**
     * @brief Returns the thread ID of the calling thread.
     *
     * @return The ID of the calling thread.
     */
    int get_running_thread();

    /**
     * this method is the signal handler for SIGVTALRM.
     * it recieves a signal sig as input.
     * if the signal is not SIGVTALRM ???
     * When the RUNNING thread is preempted, do the following:
     * If it was preempted because its quantum has expired, move it to the end of the
     * READY threads list.
     * then move the next thread in the queue of READY threads to the RUNNING state.
     * @param sig signal to handle
     */
    void timer_handler(int sig);

    int get_total_quanta_counter();
};


#endif //UTHREADS_H_SCHEDULER_H
