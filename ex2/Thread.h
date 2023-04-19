#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

#include <list>
#include <setjmp.h>
#include "uthreads.h"

typedef enum State {
    RUNNING,
    READY,
    BLOCKED,
} State;

typedef unsigned long address_t;

class Thread { // What should be here? SP, CP, something else...
    private:
        int tid,
        State state,
        list<char> stack(STACK_SIZE),
        address_t sp,
        thread_entry_point entry_point,
        address_t pc
    // type of SP --> beginning of stack
    // threads blocked by this one?

    public:
        Thread (int tid);
};


#endif //UTHREADS_H_THREAD_H
