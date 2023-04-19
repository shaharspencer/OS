#include "uthreads.h"
#include "Thread.h"
#include "Scheduler.h"
#include "init_helper.cpp"

#define SYSTEM_ERROR "system error: %s\n"
#define THREAD_LIBRARY_ERROR "thread library error: %s\n"

#define SUCCESS 0
#define FAILURE (-1)

Scheduler* scheduler;

int uthread_init (int quantum_usecs) {
    if (quantum_usecs <= 0) {
        // TODO handle error
        return FAILURE;
    }
    scheduler = new Scheduler(quantum_usecs);
    if (!scheduler) {
        // TODO handle malloc error
        return FAILURE;
    }
    return create_main_thread();
}


int uthread_spawn (thread_entry_point entry_point) {
    int tid = scheduler->get_free_tid();
    std::shared_ptr<Thread> thread = std::make_shared<Thread>(tid, entry_point);
}