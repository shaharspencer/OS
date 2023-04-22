#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

#include "uthreads.h"
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
    bool sleeping;
    char* stack;
    sigjmp_buf env;
    int quanta_counter;

public:
    Thread(int tid, thread_entry_point entry_point);
    ~Thread();

    int get_tid();

    State get_state();

    void set_state(State s);

    bool is_sleeping();

    sigjmp_buf &get_env();

    int get_quanta_counter();

    void increment_quanta_counter();


    /**
     * this method runs the thread: calls sigsetjmp and siglong jmp, and sets
     * the current state to RUNNING.
     */
    void run(){
        this->set_state(RUNNING);
        int return_val = sigsetjmp(this->get_env(), 1); //TODO check mask
        // if the return value was returned from siglongjmp, as in we called sigsetjmp before
        if (return_val != 0){
            return;
        }
        // start timer? TODO understand where
        // should there be some sognal change?
        this->increment_quanta_counter();

        siglongjmp(this->get_env(), 1); // TODO check mask
    }
};


#endif //UTHREADS_H_THREAD_H
