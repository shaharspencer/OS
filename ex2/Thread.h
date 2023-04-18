//
// Created by n318162971 on 4/18/23.
//

#ifndef UTHREADS_H_THREAD_H
#define UTHREADS_H_THREAD_H

typedef enum State {
    RUNNING,
    READY,
    BLOCKED,
} State;

class Thread { // What should be here? SP, CP, something else...
    int tid,
    State state
};


#endif //UTHREADS_H_THREAD_H
