#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include <memory>
#include <list>
#include <queue>
#include <sys/time.h>
#include "Thread.h"

class Scheduler {
private:
    const suseconds_t quantum,
    list<std::shared_ptr<Thread>> threads(MAX_THREAD_NUM),
    std::shared_ptr<Thread> running_thread,
    queue<std::shared_ptr<Thread>> ready_threads,
    list<std::shared_ptr<Thread>> blocked_threads,
    // more stuff?
    int get_free_tid ();
    bool add_thread (Thread thread);
    bool terminate (int tid);

public:
    Scheduler (int quantum_usecs);
    ~Scheduler();

};


#endif //UTHREADS_H_SCHEDULER_H
