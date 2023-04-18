#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include <sys/time.h>

class Scheduler {
    final suseconds_t quantum

    Scheduler (int quantum_usecs);
    void set_quantum (int quantum_usecs);
    suseconds_t get_quantum ();
};


#endif //UTHREADS_H_SCHEDULER_H
