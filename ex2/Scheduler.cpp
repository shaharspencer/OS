#include <sys/time.h>
#include "Scheduler.h"

#define NO_FREE_TID (-1)
#define MAIN_TID 0

Scheduler::Scheduler (int quantum_usecs) :
quantum((suseconds_t)quantum_usecs), timer({0}), total_quanta_counter(0) {
    // TODO create main thread and put as threads[0]
    spawn(main); // how?
    running_thread = MAIN_TID;
    ready_threads->pop();
    // TODO init Round-Robin
}

int Scheduler::get_free_tid() {
    for(int i = 0; i < MAX_THREAD_NUM; i++) {
        if (!threads[i]) {
            return i;
        }
    }
    return NO_FREE_TID;
}

bool Scheduler::spawn(thread_entry_point entry_point) {
    int tid = get_free_tid();
    if(tid == NO_FREE_TID) {
        throw new Error("spawn: no free tid"); // TODO remove when done
        return false;
    }
    std::shared_ptr<Thread> thread =
        std::make_shared<Thread>(tid, entry_point);
    if(!thread) {
        throw new Error("spawn: thread construction failed"); // TODO remove when done
        return false;
    }
    threads[tid] = thread;
    ready_threads.push(thread);
    return true;
}

// TODO
bool Scheduler::terminate(int tid) {
    /* Assert threads[tid] exists */
    if(!threads[tid]) {
        throw new Error("terminate: thread is null");
        return false;
    }
    /* If main thread is terminated, end run */
    if(tid == MAIN_TID) {
        // TODO implement exit w/ memory clearing
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