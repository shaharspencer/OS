#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

#include <list>
#include <setjmp.h>
#include "uthreads.h"

typedef enum State {
    RUNNING,
    READY,
    BLOCKED,
    TERMINATED
} State;

class Thread { // What should be here? SP, CP, something else...
private:
    int tid;
    State state;
    sigjmp_buf context;

public:
    Thread(int tid, thread_entry_point entry_point);

    void resume();

    sigjmp_buf &get_context();
};


#endif //UTHREADS_H_THREAD_H
