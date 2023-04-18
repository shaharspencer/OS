#include <sys/time.h>
#include "Scheduler.h"

Scheduler::Scheduler(int quantum_usecs) {
    this->set_quantum(quantum_usecs);
}

::suseconds_t Scheduler::get_quantum() {
    return
}

void Scheduler::set_quantum (int quantum_usecs) {
    this->set_quantum()
}