#include "uthreads.h"
#include "Thread.h"
#include "Scheduler.h"
#include "init_helper.cpp"

#define SYSTEM_ERROR "system error: %s\n"
#define THREAD_LIBRARY_ERROR "thread library error: %s\n"

#define SUCCESS 0
#define FAILURE (-1)

static std::unique_ptr<Scheduler> scheduler;

int uthread_init(int quantum_usecs) {
    if (quantum_usecs <= 0) {
        // TODO handle error
        return FAILURE;
    }
    scheduler = std::make_unique<Scheduler>(quantum_usecs);
    if (!scheduler) {
        // TODO handle malloc error
        return FAILURE;
    }
    return create_main_thread(scheduler);
}

int uthread_spawn(thread_entry_point entry_point) {
    int tid = scheduler->get_free_tid();
    std::shared_ptr <Thread> thread = std::make_shared<Thread>(tid, entry_point);
    bool ok = scheduler->add_thread(main_thread);
    if (!ok) {
        // TODO handle error
        return FAILURE;
    }
    return SUCCESS;
}

int uthread_terminate(int tid) {
    // if we want to destroy the main process, just exit the program
    if (tid == MAIN_TID){
        delete scheduler;
        exit(0);
    }
    if (!scheduler->terminate(tid)) {
        // TODO handle error
        return FAILURE;
    }
    return SUCCESS;
}

int uthread_block(int tid) {
    bool ok = scheduler->block(tid);
    if (!ok) {
        // TODO handle error
        return FAILURE;
    }
    return SUCCESS;
}

int uthread_resume(int tid) {
    scheduler->resume(tid) ? SUCCESS : FAILURE;
}