#include "Scheduler.h"
#include <queue>
#include <set>
#include <sys/time.h>
#include <signal.h>

#define FAILURE (-1)
#define SUCCESS 0
#define MAIN_TID 0

Scheduler::Scheduler (int quantum_usecs) :
quantum((suseconds_t)quantum_usecs), timer({0}), total_quanta_counter(0) {
    ready_threads = new queue<int>();
    blocked_threads = new set<int>();
    // TODO create main thread and put as threads[0]
    spawn(main); // how?
    running_thread = MAIN_TID;
    ready_threads->pop();
    // TODO init Round-Robin
}

Scheduler::~Scheduler() {
    // TODO implement exit method and call it
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

int Scheduler::spawn(thread_entry_point entry_point) {
    /* get free tid if available, if not fail and return */
    int tid = get_free_tid();
    if(tid == FAILURE) {
        throw new Error("spawn: no free tid"); // TODO remove when done
        return FAILURE;
    }

    /* make sure entry_point != nullptr */
    if(!entry_point) {
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
    ready_threads->push(thread);
    return tid;
}

bool Scheduler::terminate(int tid) {
    /* assert tid is valid and threads[tid] exists,
     * if not fail and return */
    if (tid == 0){
        throw new Error("tried to run scheduler's terminate on main_thread, should not have reached here.")
    }
    if(!is_tid_valid(tid) || !threads[tid]) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if(!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        return FAILURE;
    }

    /* if main thread is terminated, end run */
    if(tid == MAIN_TID) {
        // TODO implement exit_scheduler() w/ memory clearing
    }

    /* terminate depending on thread's state */
    switch(threads[tid]->get_state()) {
        case READY:
            // TODO remove from ready_threads
            break;
        case RUNNING:
            // TODO set running_thread to none value
            break;
        case BLOCKED:
            // TODO remove from blocked_threads
            break;
        default:
            if(threads[tid]->is_sleeping()) {
                // TODO handle sleeping
            }
            /* delete and nullify threads[tid] */
            delete threads[tid];
            threads[tid] = nullptr;
    }
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
            // TODO remove from ready_threads
            break;
        case RUNNING:
            // TODO scheduling should be done
            break;
        case BLOCKED:
            /* nothing needs to be done */
            return SUCCESS;
        default:
            /* change thread's state to BLOCKED and add its tid to blocked_threads */
            threads[tid]->set_state(BLOCKED);
            blocked_threads->insert(tid);
    }
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
            /* TODO remove from blocked_threads */
            break;
        default:
            /* change thread's state to READY and add its tid to ready_threads */
            threads[tid]->set_state(READY);
            ready_threads->push(tid);
    }
    return SUCCESS;
}

int Scheduler::get_running_thread() {
    return running_thread;
bool Scheduler::does_thread_exist(int tid){
    return (is_tid_valid(tid) && threads.get(i) != NULL);
}

bool Scheduler::install_signal_handler(){
    struct sigaction sa = {0};
    sa.sa_handler = &timer_handler;
    if (sigaction(SIGVTALRM, &sa, NULL) < 0)
    {
        // TODO handle error
        printf("sigaction error.");
    }
}


void Scheduler::timer_handler(int sig){
    if !(sig == SIGVTALRM){
        // DO SOMETHING
    }
    // push current thread into ready_threads




    // start next thread in queue



}

void pop_next_ready_thread(){
    // deal with the running thread: either terminate or move to ready_threads.
    if (threads[running_thread]->getState() == RUNNING){
        ready_threads.push(running_thread);
    }


    // begin running next thread in queue
    running_thread = ready_threads.pop();
    // set state of running thread to running

    // call run for thread TODO should do other check beforehand?
    threads[running_thread]->run();
}