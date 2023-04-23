/**  User Level Threads - "Fake It Until You Make It"  **/

#include "uthreads.h"
#include "Scheduler.h"

static Scheduler* scheduler; // TODO validate static use

int uthread_init(int quantum_usecs) {
    if (quantum_usecs <= 0) {
        // TODO handle error
        return FAILURE;
    }
    scheduler = new Scheduler(quantum_usecs);
    if (!scheduler) {
        // TODO handle malloc error
        return FAILURE;
    }
    return SUCCESS;
}

int uthread_spawn(thread_entry_point entry_point) {
    return scheduler->spawn(entry_point);
}

int uthread_terminate(int tid) {
    return scheduler->terminate(tid);
}

int uthread_block(int tid) {
    return scheduler->block(tid);
}

int uthread_resume(int tid) {
    return scheduler->resume(tid);
}

int uthread_sleep(int num_quantums) {
    return scheduler->sleep(num_quantums);
}

int uthread_get_tid() {
    return scheduler->get_running_thread();
}

int uthread_get_total_quantums() {
    return scheduler->get_total_quanta_counter();
}

int uthread_get_quantums(int tid) {
    return scheduler->get_quanta_counter(tid);
}