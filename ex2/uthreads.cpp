/**  User Level Threads - "Fake It Until You Make It"  **/

#include "uthreads.h"
#include "Scheduler.h"

#include <iostream>

#define SYSTEM_ERROR_EXIT 1

Scheduler *scheduler;

int handle_error(std::string error_msg) {
    std::cerr << error_msg;
    if (error_msg.substr(0, SYSTEM_ERROR.size()) == SYSTEM_ERROR) {
        if (scheduler) {
            delete scheduler;
        }
        exit(SYSTEM_ERROR_EXIT);
    }
    return FAILURE;
}

int uthread_init(int quantum_usecs) {
    if (quantum_usecs <= 0) {
        return handle_error(THREAD_LIBRARY_ERROR + "negative seconds in init\n");
    }

    try {
        scheduler = new Scheduler(quantum_usecs);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }

    if (!scheduler) {
        return handle_error(SYSTEM_ERROR + "could not create scheduler\n");
    }
    return SUCCESS;
}

int uthread_spawn(thread_entry_point entry_point) {
    try {
        return scheduler->spawn(entry_point);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}

int uthread_terminate(int tid) {
    if (tid == MAIN_TID) {
        delete scheduler;
        exit(MAIN_TID);
    }
    try {
        return scheduler->terminate(tid);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}

int uthread_block(int tid) {
    try {
        return scheduler->block(tid);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}

int uthread_resume(int tid) {
    try {
        return scheduler->resume(tid);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}

int uthread_sleep(int num_quantums) {

    try {
        return scheduler->sleep(num_quantums);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}

int uthread_get_tid() {
    return scheduler->get_running_thread();
}

int uthread_get_total_quantums() {
    return scheduler->get_total_quanta_counter();
}

int uthread_get_quantums(int tid) {
    try {
        return scheduler->get_quanta_counter(tid);
    }
    catch (const std::invalid_argument &e) {
        return handle_error(e.what());
    }
    catch (const std::system_error &e) {
        return handle_error(e.what());
    }
}