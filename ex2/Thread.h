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

class Thread {
private:
    int tid;
    State state;
    sigjmp_buf context;

public:
    Thread(int tid, thread_entry_point entry_point);

    void resume();

    int get_tid();

    State get_state();

    sigjmp_buf &get_context();
};


#endif //UTHREADS_H_THREAD_H
