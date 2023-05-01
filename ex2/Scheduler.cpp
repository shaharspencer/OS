#include "Scheduler.h"


Scheduler *Scheduler::instance = nullptr;

Scheduler::Scheduler(int quantum_usecs) :
        quantum((suseconds_t) quantum_usecs),
        timer({0, 0, 0, 0}), total_quanta_counter(0) {

    instance = this;

    /* define SIGVTALM to block/unblock during scheduling actions */
    if (sigemptyset(&signals)) {
        throw std::system_error(errno, std::generic_category(), SYSTEM_ERROR + "sigemptyset failed\n");
    }
    if (sigaddset(&signals, SIGVTALRM)) {
        throw std::system_error(errno, std::generic_category(), SYSTEM_ERROR + "sigaddset failed\n");
    }
    if (sigprocmask(SIG_SETMASK, &signals, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "sigprocmask failed\n");
    }

    /* create data structures */
    ready_threads = new std::deque<int>();
    blocked_threads = new std::set<int>();
    sleeping_threads = new std::set<int>();

    /* create main thread and schedule it immediately */
    try {
        spawn(MAIN_TID);
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* immediately set main to running without scheduling */
    threads[MAIN_TID]->set_state(RUNNING);
    running_thread = MAIN_TID;
    ready_threads->pop_front();

    /* immediately schedule main thread */
    schedule();
}

Scheduler::~Scheduler() {
    /* deletes all created threads and structures */
    instance = nullptr;
    for (auto &thread: threads) {
        if (thread) {
            delete thread;
            thread = nullptr;
        }
    }
    delete ready_threads;
    delete blocked_threads;
    delete sleeping_threads;
}

int Scheduler::get_free_tid() {
    for (int i = 0; i < MAX_THREAD_NUM; i++) {
        if (!threads[i]) {
            return i;
        }
    }
    return FAILURE;
}

bool Scheduler::is_tid_valid(int tid) {
    return tid >= 0 && tid < MAX_THREAD_NUM && threads[tid];
}

void Scheduler::remove_from_ready(int tid) {
    /* assert tid validity */
    if (!is_tid_valid(tid)) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + "ready remove: tid invalid\n");
    }

    /* advance along ready_threads, if current thread has same tid remove it */
    for (auto it = ready_threads->begin(); it != ready_threads->end(); it++) {
        if (*it == tid) {
            ready_threads->erase(it);
            return;
        }
    }
    throw std::invalid_argument(THREAD_LIBRARY_ERROR + "tid not in ready\n");
}

int Scheduler::spawn(thread_entry_point entry_point) {
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* get free tid if available, if not fail and return */
    int tid = get_free_tid();
    if (tid == FAILURE) {
        try {
            sigprocmask_unblock();
            throw std::invalid_argument(THREAD_LIBRARY_ERROR + "spawn: no free tid\n");
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* make sure entry_point != nullptr */
    if (tid != MAIN_TID && entry_point == nullptr) {
        try {
            sigprocmask_unblock();
            throw std::invalid_argument(THREAD_LIBRARY_ERROR + "spawn: entry_point == nullptr\n");
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* create a new Thread Control Block (TCB).
     * if creation failed, fail and return */
    auto thread = (Thread *) nullptr;
    try {
        thread = new Thread(tid, entry_point);
    }
    catch (const std::system_error &e) {
        throw e;
    }

    if (!thread) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "spawn - thread construction failed\n");
    }

    /* assign new thread to threads list */
    threads[tid] = thread;
    /* since spawned thread is READY, push its tid to ready queue */
    ready_threads->push_back(tid);
    try {
        sigprocmask_unblock();
        return tid;
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

int Scheduler::terminate(int tid) {
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }


    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        try {
            sigprocmask_unblock();
            throw std::invalid_argument(THREAD_LIBRARY_ERROR +
                                        "tried to terminate invalid tid\n");
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* terminate depending on thread's state */
    if (threads[tid]->get_sleeping_time() != AWAKE) {
        sleeping_threads->erase(tid);
        try {
            sigprocmask_unblock();
            return SUCCESS;
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }
    switch (threads[tid]->get_state()) {
        case READY:
            try {
                remove_from_ready(tid);
            }

            catch (const std::invalid_argument &e) {
                throw e;
            }
            catch (const std::system_error &e) {
                throw e;
            }
            break;
        case BLOCKED:
            blocked_threads->erase(tid);
            break;
        case RUNNING:
            previous_thread = running_thread;
            running_thread = PREEMPTED;
            try {
                schedule();
            }
            catch (const std::invalid_argument &e) {
                throw e;
            }
            catch (const std::system_error &e) {
                throw e;
            }
    }

    /* dealloc and nullify threads[tid] */
    delete threads[tid];
    threads[tid] = nullptr;

    try {
        sigprocmask_unblock();
        return SUCCESS;
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

int Scheduler::block(int tid) {
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + "terminate: tid is invalid or thread is nullptr\n");
    }

    /* assert main thread isn't being blocked */
    if (tid == MAIN_TID) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + "terminate - can't block main thread\n");
    }

    /* save thread's current state for later structure handling */
    State curr_state = threads[tid]->get_state();
    /* change thread's state to BLOCKED */
    threads[tid]->set_state(BLOCKED);

    /* change data structure depending on thread's state */
    if (threads[tid]->get_sleeping_time() == AWAKE) {
        switch (curr_state) {
            case READY:
                remove_from_ready(tid);
                blocked_threads->insert(tid);
                break;
            case BLOCKED:
                /* nothing needs to be done */
                break;
            case RUNNING:
                previous_thread = running_thread;
                running_thread = PREEMPTED;
                blocked_threads->insert(tid);
                /* thread's PC increments so that on resume it won't re-block itself  */
                if (threads[tid]->thread_sigsetjmp() == 0) {
                    schedule();
                }
                break;
        }
    }
    try {
        sigprocmask_unblock();
        return SUCCESS;
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

int Scheduler::resume(int tid) {
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + "terminate: tid is invalid or thread is nullptr\n");
    }

    /* save thread's current state for later structure handling */
    State curr_state = threads[tid]->get_state();
    /* change thread's state to READY */
    threads[tid]->set_state(READY);

    /* change data structure depending on thread's state */
    if (threads[tid]->get_sleeping_time() == AWAKE) {
        switch (curr_state) {
            case READY:
                /* nothing needs to be done */
                break;
            case BLOCKED:
                /* remove from blocked_threads */
                blocked_threads->erase(tid);
                ready_threads->push_back(tid);
                break;
            case RUNNING:
                /* usage error - ignore and schedule the next ready thread */
                schedule();
                break;
        }
    }
    try {
        sigprocmask_unblock();
        return SUCCESS;
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

int Scheduler::sleep(int num_quanta) {
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* assert num_quanta is valid, if not fail and return */
    if (num_quanta <= 0) {
        try {
            sigprocmask_unblock();
            throw std::invalid_argument(THREAD_LIBRARY_ERROR + "sleep: negative time\n");
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* assert running thread isn't main thread, if so fail and return */
    if (running_thread == MAIN_TID) {
        try {
            sigprocmask_unblock();
            throw std::invalid_argument(THREAD_LIBRARY_ERROR + "sleep - can't put main thread to sleep\n");
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* set thread's state to READY and its sleeping timer */
    threads[running_thread]->set_state(READY);
    threads[running_thread]->set_sleeping_time(num_quanta);

    /* change data structure */
    sleeping_threads->insert(running_thread);
    previous_thread = running_thread;
    running_thread = PREEMPTED;
    /* thread's PC increments so that on resume it won't re-sleep itself  */
    if (threads[previous_thread]->thread_sigsetjmp() == 0) {
        schedule();
    }

    try {
        sigprocmask_unblock();
        return SUCCESS;
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

void Scheduler::schedule() {
    /* install timer_handler as action for SIGVTALRM */
    struct sigaction sa;
    sa.sa_handler = &Scheduler::static_timer_handler;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + " schedule - sigaction error\n");
    }

    /* set the timer for given quantum */
    timer.it_value.tv_sec = quantum / 1000000;
    timer.it_value.tv_usec = quantum % 1000000;
    timer.it_interval.tv_sec = quantum / 1000000;
    timer.it_interval.tv_usec = quantum % 1000000;

    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "timer init: setitimer failed\n");
    }

    /* force context switch to occur */
    timer_handler(SIGVTALRM);
}

/**
 * this method is the signal handler for SIGVTALRM.
 * it recieves a signal sig as input.
 * if the signal is not SIGVTALRM ???
 * When the RUNNING thread is preempted, do the following:
 * If it was preempted because its quantum has expired, move it to the end of the
 * READY threads list.
 * then move the next thread in the queue of READY threads to the RUNNING state.
 * @param sig signal to handle
 */

void Scheduler::timer_handler(int sig) {
    /* block signals with sigprocmask */
    try {
        sigprocmask_block();
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* assert signal is correct, if not fail and return */
    if (sig != SIGVTALRM) {
        try {
            sigprocmask_unblock();
            return;
        }
        catch (const std::system_error &e) {
            throw e;
        }
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "timer handler: wrong signal\n");
    }

    /* if running thread isn't terminated, push to ready_threads and setjmp */
    if (running_thread != PREEMPTED) {
        threads[running_thread]->set_state(READY);
        ready_threads->push_back(running_thread);

        /* case where sigsetjmp succeeded with return value 0 */
        if (threads[running_thread]->thread_sigsetjmp() != 0) {
            try {
                sigprocmask_unblock();
                return;
            }
            catch (const std::system_error &e) {
                throw e;
            }
        }
    }

    /* if no threads are awaiting execution, do nothing */
    if (ready_threads->empty()) {
        try {
            sigprocmask_unblock();
            return;
        }
        catch (const std::system_error &e) {
            throw e;
        }
    }

    /* set next ready thread as new running thread */
    running_thread = ready_threads->front();
    ready_threads->pop_front();
    threads[running_thread]->set_state(RUNNING);

    /* increment both thread's and total quanta counters */
    if (running_thread == PREEMPTED) {
        threads[previous_thread]->increment_quanta_counter();
    } else {
        threads[running_thread]->increment_quanta_counter();
    }
    increment_total_quanta_counter();

    /* manage the sleeping threads */
    try {
        handle_sleeping_threads();
    }
    catch (const std::invalid_argument &e) {
        throw e;
    }
    catch (const std::system_error &e) {
        throw e;
    }

    /* set timer and assert success */
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "timer handler: setitimer failed\n");
    }

    /* finally, perform the longjmp to new running thread */
    try {
        sigprocmask_unblock();
        threads[running_thread]->thread_siglongjmp(1);
        sigprocmask_unblock();
    }
    catch (const std::system_error &e) {
        throw e;
    }
}

/** decrement sleeping time of sleeping threads,
  * if a thread reaches AWAKE remove it from sleeping */
void Scheduler::handle_sleeping_threads() {
    /* a set to hold tids of threads in need to wake up */
    auto wake_up = new std::set<int>();

    /* for every sleeping thread, decrement its quanta remainder by 1 */
    for (int sleeping_tid: *sleeping_threads) {
        threads[sleeping_tid]->decrement_sleeping_time();
        /* if time remaining to sleep is 0, add to wake up */
        if (threads[sleeping_tid]->get_sleeping_time() == AWAKE) {
            wake_up->insert(sleeping_tid);
        }
    }

    /* wake up threads that finished sleeping */
    for (int wake_up_tid: *wake_up) {
        /* remove from sleeping_threads */
        sleeping_threads->erase(wake_up_tid);
        /* wake up to correct state */
        switch (threads[wake_up_tid]->get_state()) {
            case READY:
                ready_threads->push_back(wake_up_tid);
                break;
            case BLOCKED:
                blocked_threads->insert(wake_up_tid);
                return;
            case RUNNING:
                printf("sleeping handle: can't wake up running "
                       "thread");
        }
    }
}

int Scheduler::get_quanta_counter(int tid) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + INVALID_ARG +
                                    "get_quanta_counter - tid is invalid or thread is nullptr\n");
    }

    return threads[tid]->get_quanta_counter();
}

void Scheduler::sigprocmask_block() {
    if (sigprocmask(SIG_BLOCK, &signals, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + "sigprocmask block failed\n");
    }
}

void Scheduler::sigprocmask_unblock() {
    if (sigprocmask(SIG_UNBLOCK, &signals, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(),
                                SYSTEM_ERROR + " sigprocmask unblock failed\n");
    }
}