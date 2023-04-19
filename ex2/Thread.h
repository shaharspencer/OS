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
    int tid;
    State state;
//    list<char> stack;
//    address_t sp;
//    thread_entry_point entry_point;
//    address_t pc;
    sigjmp_buf context;

public:
    Thread(int tid, thread_entry_point entry_point);

    void resume();

    sigjmp_buf &get_context();
};


#endif //UTHREADS_H_THREAD_H
