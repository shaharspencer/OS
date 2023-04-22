#include "Scheduler.h"
#include <queue>
#include <set>
#include <sys/time.h>
#include <signal.h>

#define FAIL (-1)
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
    return FAIL;
}

bool Scheduler::is_tid_valid(int tid) {
    return tid >= 0 && tid < MAX_THREAD_NUM;
}

int Scheduler::spawn(thread_entry_point entry_point) {
    /* get free tid if available, if not fail and return */
    int tid = get_free_tid();
    if(tid == FAIL) {
        throw new Error("spawn: no free tid"); // TODO remove when done
        return FAIL;
    }

    /* make sure entry_point != nullptr */
    if(!entry_point) {
        throw new Error("spawn: entry_point == nullptr"); // TODO remove later?
        return FAIL;
    }

    /* create a new Thread Control Block (TCB).
     * if creation failed, fail and return */
    auto thread = new Thread(tid, entry_point);
    if(!thread) {
        throw new Error("spawn: thread construction failed"); // TODO remove when done
        return FAIL;
    }

    /* assign new thread to threads list */
    threads[tid] = thread;
    /* since spawned thread is READY, push its tid to ready queue */
    ready_threads.push(thread);
    return tid;
}

bool Scheduler::terminate(int tid) {
    /* assert tid is valid and threads[tid] exists,
     * if not fail and return */
    if(!is_tid_valid(tid) || !threads[tid]) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        return false;
    }

    /* if main thread is terminated, end run */
    if(tid == MAIN_TID) {
        // TODO implement exit_scheduler() w/ memory clearing
    }

    /* Terminate depending on thread's state */
    switch (threads[tid]->get_state())
    {
        case READY:
            break;
        case RUNNING:
            break;
        case BLOCKED:
            break;
        default:
            break; // sleeping
    }
}

bool Scheduler::does_thread_exist(int tid){
    return (threads.get(i) != NULL);
}

bool install_signal_handler(){
    struct sigaction sa = {0};
    sa.sa_handler = &timer_handler;
    if (sigaction(SIGVTALRM, &sa, NULL) < 0)
    {
        // TODO handle error
        printf("sigaction error.");
    }
}


bool timer_handler(int sig){
    if !(sig == SIGVTALRM){
        // DO SOMETHING
    }
    // terminate current thread -though perhaps it terminates itself?

    // start next thread in queue

}