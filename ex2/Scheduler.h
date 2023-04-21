#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include "Thread.h"
#include <memory>
#include <list>
#include <queue>
#include <set>
#include <sys/time.h>
#include <signal.h>
#include <error.h>

class Scheduler {
private:
    /* Timer components */
    const suseconds_t quantum;
    struct itimer timer;
    int total_quanta_counter;

    /* Threads components */
    list <std::shared_ptr<Thread>> threads;
    std::shared_ptr <Thread> running_thread;
    queue <std::shared_ptr<Thread>> ready_threads;
    set <std::shared_ptr<Thread>> blocked_threads;

    /* Signals component */
    sigset_t signals;

    void timer_handler(int sig);

public:
    /**
     * constructor for Scheduler
     * @param quantum_usecs the value of a quantam in micro-seconds
     */
    Scheduler(int quantum_usecs);
    ~Scheduler();

    /**
    * gets the next free id
    * @return id of free thread, -1 if there are none
    **/
    int get_free_tid();

    /**
     * recieves a thread and adds it to threads and to ready_threads
     * * @param thread thread to add
     * @return true if success else false
    **/
    bool add_thread(std::shared_ptr <Thread> thread);

    /** removes a thread from threads
     * @param tid id of thread to remove
     * @return true if success else false
     */
    bool terminate(int tid);

    bool does_thread_exist(int tid);

    bool block(int tid);

    bool yield();

};


#endif //UTHREADS_H_SCHEDULER_H
