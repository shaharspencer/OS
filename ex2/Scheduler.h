#ifndef UTHREADS_H_SCHEDULER_H
#define UTHREADS_H_SCHEDULER_H

#include <sys/time.h>

class Scheduler {
    const suseconds_t quantum;
    // more stuff?

    public:
        Scheduler (int quantum_usecs);
};


#endif //UTHREADS_H_SCHEDULER_H
