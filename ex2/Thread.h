#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

#include "uthreads.h"

#include <setjmp.h>
#include <signal.h>

#define AWAKE 0

typedef enum State {
    RUNNING,
    READY,
    BLOCKED,
} State;

class Thread {
private:
    int tid;
    State state;
    int sleeping_time;
    char* stack;
    sigjmp_buf env;
    int quanta_counter;

public:
    Thread(int tid, thread_entry_point entry_point);
    ~Thread();

    int get_tid();

    State get_state();

    void set_state(State s);

    int get_sleeping_time() { return sleeping_time; }

    void set_sleeping_time(int num_quanta) { sleeping_time = num_quanta; }

    void decrement_sleeping_time() { sleeping_time--; }

    int get_quanta_counter();

    void increment_quanta_counter();

    int thread_sigsetsetjmp();

    void thread_siglongjmp(int val);
};


#endif //UTHREADS_H_THREAD_H
