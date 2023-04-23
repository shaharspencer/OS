#include "Scheduler.h"
#include <deque>
#include <set>
#include <sys/time.h>
#include <signal.h>

using namespace std;

Scheduler::Scheduler (int quantum_usecs) :
quantum((suseconds_t)quantum_usecs), timer({0}), total_quanta_counter(0) {
    ready_threads = new deque<int>();
    blocked_threads = new set<int>();
    // TODO create main thread and put as threads[0]
    spawn(MAIN_TID);
    running_thread = MAIN_TID;
    ready_threads->pop_front();
    // TODO init Round-Robin
}

Scheduler::~Scheduler() {
    /* deletes all created threads */
    for(int i = 0; i < MAX_THREAD_NUM; i++) {
        delete threads[i];
    }
}

int Scheduler::get_free_tid() {
    for(int i = 0; i < MAX_THREAD_NUM; i++) {
        if (!threads[i]) {
            return i;
        }
    }
    return FAILURE;
}

bool Scheduler::is_tid_valid(int tid) {
    return tid >= 0 && tid < MAX_THREAD_NUM && threads[tid];
}

void Scheduler::remove_from_ready(int tid) {
    /* assert tid validity */
    if(!is_tid_valid(tid)) {
        throw new Error("ready remove: tid invalid");
        return;
    }
    /* advance along ready_threads, if current thread has same tid remove it */
    for(auto it = ready_threads->begin(); it != ready_threads->end(); it++) {
        if(*it == tid) {
            ready_threads->erase(it);
            return;
        }
    }
    throw new Error("ready remove: tid not in ready");
}

int Scheduler::spawn(thread_entry_point entry_point) {
    /* get free tid if available, if not fail and return */
    int tid = get_free_tid();
    if(tid == FAILURE) {
        throw new Error("spawn: no free tid"); // TODO remove when done
        return FAILURE;
    }

    /* make sure entry_point != nullptr */
    if(entry_point == nullptr) {
        throw new Error("spawn: entry_point == nullptr"); // TODO remove later?
        return FAILURE;
    }

    /* create a new Thread Control Block (TCB).
     * if creation failed, fail and return */
    auto thread = new Thread(tid, entry_point);
    if(!thread) {
        throw new Error("spawn: thread construction failed"); // TODO remove when done
        return FAILURE;
    }

    /* assign new thread to threads list */
    threads[tid] = thread;
    /* since spawned thread is READY, push its tid to ready queue */
    ready_threads->push_back(tid);
    return tid;
}

bool Scheduler::terminate(int tid) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if(!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        return FAILURE;
    }

    /* if main thread is terminated, end run */
    if(tid == MAIN_TID) {
        // shouldn't happen
        throw new Error("terminate: something weird happened, called with "
                        "tid == MAIN_TID")
    }

    /* terminate depending on thread's state */
    switch(threads[tid]->get_state()) {
        case READY:
            remove_from_ready(tid);
            break;
        case RUNNING:
            running_thread = RUNNING_THREAD_TERMINATED;
            // call time_handler
            break;
        case BLOCKED:
            blocked_threads->erase(tid);
            break;
    }
    if(threads[tid]->is_sleeping()) {
        // TODO handle sleeping
    }
    /* delete and nullify threads[tid] */
    delete threads[tid];
    threads[tid] = nullptr;
    return SUCCESS;
}

int Scheduler::block(int tid) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if(!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        return FAILURE;
    }

    /* assert main thread isn't being blocked */
    if(tid == MAIN_TID) {
        throw new Error("terminate: can't block main thread");
        return FAILURE;
    }

    /* block depending on thread's state */
    switch(threads[tid]->get_state()) {
        case READY:
            remove_from_ready(tid);
            break;
        case RUNNING:
            // TODO scheduling should be done - involve timer_handler?
            break;
        case BLOCKED:
            /* nothing needs to be done */
            return SUCCESS;
    }
    /* change thread's state to BLOCKED and add its tid to blocked_threads */
    threads[tid]->set_state(BLOCKED);
    blocked_threads->insert(tid);
    return SUCCESS;
}

int Scheduler::resume(int tid) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if(!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        return FAILURE;
    }

    /* resume depending on thread's state */
    switch(threads[tid]->get_state()) {
        case READY:
        case RUNNING:
            /* nothing needs to be done */
            return SUCCESS;
        case BLOCKED:
            /* remove from blocked_threads */
            blocked_threads->erase(tid);
            break;
    }
    /* change thread's state to READY and add its tid to ready_threads */
    threads[tid]->set_state(READY);
    ready_threads->push_back(tid);
    return SUCCESS;
}

int Scheduler::get_running_thread() {
    return running_thread;
}

bool Scheduler::install_timer_handler(){
    struct sigaction sa = {0};
    sa.sa_handler = &timer_handler;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0)
    {
        // TODO handle error
        printf("sigaction error.");
        return false;
    }
    return true;
}


void Scheduler::timer_handler(int sig){
    /* assert signal is correct, if not fail and return */
    if(sig != SIGVTALRM) {
        throw new Error("timer handler: wrong signal");
        return;
    }

    /* if running thread isn't terminated, push to ready_threads and setjmp */
    if(running_thread != RUNNING_THREAD_TERMINATED) {
        ready_threads->push_back(running_thread);
        threads[running_thread]->set_state(READY); // TODO make sure all
        // states change according to our reassignments
        /* assert sigsetjmp succeeded */
        if(threads[running_thread]->setjmp() != 0) {
            throw new Error("timer handler: setjmp failed");
            return;
        }
    }

    /* if no threads are awaiting execution, do nothing */
    if(ready_threads->empty()) {
        return;
    }

    /* set timer for quantum and longjmp to next thread to run */
    running_thread = ready_threads->front();
    threads[running_thread]->set_state(RUNNING);
    ready_threads->pop_front();

    /* increment both thread's and total quanta counters */
    threads[running_thread]->increment_quanta_counter();
    increment_total_quanta_counter();

    /* set timer and assert success */
    if(setitimer(ITIMER_VIRTUAL, &timer, nullptr) == 0) {
        throw new Error("timer handler: setitimer failed");
    }

    /* finally, perform the longjmp to new running thread */
    threads[running_thread]->longjmp(1);
}