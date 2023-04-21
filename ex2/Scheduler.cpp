#include <sys/time.h>
#include "Scheduler.h"

Scheduler::Scheduler (int quantum_usecs) :
quantum((suseconds_t)quantum_usecs), timer({0}), total_quanta_counter(0) {
    // What else should be done here?
}

int Scheduler::get_free_tid (){
    // get first Null in threads
    // TODO: 0 should not be available?
    for (int i = 0; i < MAX_THREAD_NUM; i++)
    {
        if (threads.get(i) == NULL){
            return i;
        }
    }
    return -1;
}

bool Scheduler::add_thread(std::shared_ptr<Thread> thread){
    // check thread id does not exist already
    if (does_thread_exist(thread->get_id())){
        throw new Error("thread id already exists"); // TODO remove when we're done
    }
    // add to threads
    threads[thread->get_id()] = thread;
    // add to ready_threads
    ready_threads.push(thread);
    return true;
}

// TODO
bool Scheduler::terminate(int tid){
    // remove from threads

}

bool Scheduler::does_thread_exist(int tid){
    return (threads.get(i) != NULL);
}