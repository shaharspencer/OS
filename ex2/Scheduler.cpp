#include <sys/time.h>
#include "Scheduler.h"

Scheduler::Scheduler (int quantum_usecs): quantum((suseconds_t) quantum_usecs) {
    // What else should be done here?
}
