#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

#include "uthreads.h"
#include <list>
#include <setjmp.h>

typedef enum State {
    RUNNING,
    READY,
    BLOCKED,
} State;

class Thread {
private:
    int tid;
    State state;
    char* stack;
    sigjmp_buf context;
    int quantum_counter;

public:
    Thread(int tid, thread_entry_point entry_point);
    ~Thread();

    void resume();

    int get_tid();

    State get_state();

    sigjmp_buf &get_context();
};


#endif //UTHREADS_H_THREAD_H
