#include "uthreads.h"
#include "Thread.h"
#include "Scheduler.h"

#define SYSTEM_ERROR "system error: %s\n"
#define THREAD_LIBRARY_ERROR "thread library error: %s\n"

#define SUCCESS 0
#define FAILURE (-1)

Scheduler* scheduler;

int uthread_init (int quantum_usecs) {
    scheduler = new Scheduler(quantum_usecs);
    std::shared_ptr<Thread> main_thread = std::make_shared<Thread>(0, main); // TODO how to init thread 0 to main?
    scheduler->add_thread(main_thread);
    return SUCCESS; // TODO handle error
}

int uthread_spawn (thread_entry_point entry_point) {
    int tid = scheduler->get_free_tid();
    std::shared_ptr<Thread> thread = std::make_shared<Thread>(tid, entry_point);
}