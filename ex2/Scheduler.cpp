#include "Scheduler.h"

// TODO define an exit func that deallocates all memory and exits with
//  specific code, later add call to exit whenever needed

Scheduler::Scheduler(int quantum_usecs) :
        quantum((suseconds_t) quantum_usecs), timer({0, 0, 0, 0}), total_quanta_counter(0) {

    /* create data structures */
    ready_threads = new std::deque<int>();
    blocked_threads = new std::set<int>();
    sleeping_threads = new std::set<int>();

    /* create main thread and schedule it immediately */
    spawn(MAIN_TID);
    running_thread = MAIN_TID;
    threads[MAIN_TID]->set_state(RUNNING);
    ready_threads->pop_front();

    /* define signals to block/unblock during scheduling actions */
    sigemptyset(&signals);
    sigaddset(&signals, SIGVTALRM);
    /* TODO figure out what is actually happening here */
    if (sigprocmask(SIG_SETMASK, &signals, nullptr) < 0) {
        std::cerr << SYSTEM_ERROR << "sigprocmask failed" << std::endl;
        // TODO exit and dealloc memory
        return;
    }

    /* immediately schedule main thread */
    schedule();
}

Scheduler::~Scheduler() {
    /* deletes all created threads and structures */
    for (int i = 0; i < MAX_THREAD_NUM; i++) {
        if (threads[i]) {
            delete threads[i];
            threads[i] = nullptr;
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
        throw new Error("ready remove: tid invalid");
        return;
    }

    /* advance along ready_threads, if current thread has same tid remove it */
    for (auto it = ready_threads->begin(); it != ready_threads->end(); it++) {
        if (*it == tid) {
            ready_threads->erase(it);
            return;
        }
    }
    throw new Error("ready remove: tid not in ready");
}

int Scheduler::spawn(thread_entry_point entry_point) {
    sigprocmask_block();

    /* get free tid if available, if not fail and return */
    int tid = get_free_tid();
    if (tid == FAILURE) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR);
        new std::exception() Error("spawn: no free tid"); // TODO remove when done
        sigprocmask_unblock();
        return FAILURE;
    }

    /* make sure entry_point != nullptr */
    if (entry_point == nullptr) {
        throw new Error("spawn: entry_point == nullptr"); // TODO remove later?
        sigprocmask_unblock();
        return FAILURE;
    }

    /* create a new Thread Control Block (TCB).
     * if creation failed, fail and return */
    auto thread = new Thread(tid, entry_point);
    if (!thread) {
        throw new Error("spawn: thread construction failed"); // TODO remove when done
        return FAILURE;
    }

    /* assign new thread to threads list */
    threads[tid] = thread;
    /* since spawned thread is READY, push its tid to ready queue */
    ready_threads->push_back(tid);
    sigprocmask_unblock();
    return tid;
}

int Scheduler::terminate(int tid) {
    sigprocmask_block();

    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        // TODO throw library error
        sigprocmask_unblock();
        return FAILURE;
    }

    /* if main thread is terminated, ends run from outer scope */

    /* dealloc and nullify threads[tid] */
    delete threads[tid];
    threads[tid] = nullptr;

    /* terminate depending on thread's state */
    if (threads[tid]->get_sleeping_time() != AWAKE) {
        sleeping_threads->erase(tid);
        sigprocmask_unblock();
        return SUCCESS;
    }
    switch (threads[tid]->get_state()) {
        case READY:
            remove_from_ready(tid);
            break;
        case BLOCKED:
            blocked_threads->erase(tid);
            break;
        case RUNNING:
            running_thread = PREEMPTED;
            schedule();
    }
    sigprocmask_unblock();
    return SUCCESS;
}

int Scheduler::block(int tid) {
    sigprocmask_block();

    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        sigprocmask_unblock();
        return FAILURE;
    }

    /* assert main thread isn't being blocked */
    if (tid == MAIN_TID) {
        throw new Error("terminate: can't block main thread");
        sigprocmask_unblock();
        return FAILURE;
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
                running_thread = PREEMPTED;
                blocked_threads->insert(tid);
                schedule();
                break;
        }
    }
    sigprocmask_unblock();
    return SUCCESS;
}

int Scheduler::resume(int tid) {
    sigprocmask_block();

    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw new Error("terminate: tid is invalid or thread is nullptr");
        sigprocmask_unblock();
        return FAILURE;
    }

    /* save thread's current state for later structure handling */
    State curr_state = threads[tid]->get_state();
    /* change thread's state to READY */
    threads[tid]->set_state(READY);

    /* change data structure depending on thread's state */
    if (threads[tid]->get_sleeping_time() == AWAKE) {
        switch (threads[tid]->get_state()) {
            case RUNNING:
            case READY:
                /* nothing needs to be done */
                sigprocmask_unblock();
                return SUCCESS;
            case BLOCKED:
                /* remove from blocked_threads */
                blocked_threads->erase(tid);
                ready_threads->push_back(tid);
                break;
        }
    }
    sigprocmask_unblock();
    return SUCCESS;
}

int Scheduler::sleep(int num_quanta) {
    sigprocmask_block();

    /* assert num_quanta is valid, if not fail and return */
    if (num_quanta < 0) {
        throw new Error("sleep: negative time");
        sigprocmask_unblock();
        return FAILURE;
    }

    /* assert running thread isn't main thread, if so fail and return */
    if (running_thread == MAIN_TID) {
        throw new Error("sleep: can't put main thread to sleep");
        sigprocmask_unblock();
        return FAILURE;
    }

    /* set thread's state to READY and its sleeping timer */
    threads[running_thread]->set_state(READY);
    threads[running_thread]->set_sleeping_time(num_quanta);

    /* change data structure */
    sleeping_threads->insert(running_thread);
    running_thread = PREEMPTED;
    schedule();
    sigprocmask_unblock();
    return SUCCESS;
}

void Scheduler::schedule() {
    sigprocmask_block();

    /* install timer_handler as action for SIGVTALRM */
    struct sigaction sa = {0};
    sa.sa_handler = &timer_handler;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0) {
        // TODO handle system error
        printf("sigaction error.");
        sigprocmask_unblock();
        return;
    }

    /* set the timer for given quantum */
    timer.it_value.tv_sec = quantum / 1e6;
    timer.it_value.tv_usec = quantum % 1e6;
    timer.it_interval.tv_sec = quantum / 1e6;
    timer.it_interval.tv_usec = quantum % 1e6;
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
        std::cerr << SYSTEM_ERROR << "setitimer failed" << std::endl;
        throw new Error("timer init: setitimer failed.");
        // TODO exit
        sigprocmask_unblock();
        return;
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
    sigprocmask_block();

    /* assert signal is correct, if not fail and return */
    if (sig != SIGVTALRM) {
        // TODO unblock
        throw new Error("timer handler: wrong signal"); // TODO remove later
        sigprocmask_unblock();
        return;
    }

    /* if running thread isn't terminated, push to ready_threads and setjmp */
    if (running_thread != PREEMPTED) {
        ready_threads->push_back(running_thread);
        threads[running_thread]->set_state(READY);

        /* case where sigsetjmp succeeded with return value 0 */
        if (threads[running_thread]->thread_sigsetsetjmp() != 0) {
            sigprocmask_unblock();
            return;
        }
    }

    /* if no threads are awaiting execution, do nothing */
    if (ready_threads->empty()) {
        sigprocmask_unblock();
        return;
    }

    /* set next ready thread as new running thread */
    running_thread = ready_threads->front();
    threads[running_thread]->set_state(RUNNING);
    ready_threads->pop_front();

    /* increment both thread's and total quanta counters */
    threads[running_thread]->increment_quanta_counter();
    increment_total_quanta_counter();

    /* manage the sleeping threads */
    handle_sleeping_threads();

    /* set timer and assert success */
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
        throw new std::system_error(SYSTEM_ERROR + "timer handler: setitimer failed\n");
    }

    /* finally, perform the longjmp to new running thread */
    sigprocmask_unblock();
    threads[running_thread]->thread_siglongjmp(1);
}

/** decrement sleeping time of sleeping threads,
  * if a thread reaches AWAKE remove it from sleeping */
void Scheduler::handle_sleeping_threads() {
    /* a set to hold tids of threads in need to wake up */
    auto wake_up = new set<int>();  // TODO seems like a problem ):

    /* for every sleeping thread, decrement its quanta remainder by 1 */
    for (int *it: sleeping_threads) {
        threads[*it]->decrement_sleeping_time();
        /* if time remaining to sleep is 0, add to wake up */
        if (threads[*it]->get_sleeping_time() == AWAKE) {
            wake_up->insert(*it);
        }
    }

    /* wake up threads that finished sleeping */
    for (int *it: wake_up) {
        /* remove from sleeping_threads */
        sleeping_threads->erase(*it);
        /* wake up to correct state */
        switch (threads[*it]->get_state()) {
            case READY:
                ready_threads->push_back(*it);
                break;
            case BLOCKED:
                blocked_threads->insert(*it);
                break;
            case RUNNING:
                throw std::exception("sleeping handle: can't wake up running "
                                     "thread")
        }
    }
}

int Scheduler::get_quanta_counter(int tid) {
    /* assert tid is valid and threads[tid] exists, if not fail and return */
    if (!is_tid_valid(tid)) {
        throw std::invalid_argument(THREAD_LIBRARY_ERROR + INVALID_ARG +
        "terminate: tid is invalid or thread is nullptr\n" )
    }

    return threads[tid]->get_quanta_counter();
}

void Scheduler::sigprocmask_block() {
    if (sigprocmask(SIG_BLOCK, &signals, nullptr) < 0) {
        std::cerr << SYSTEM_ERROR << "sigprocmask failed" << std::endl;
        // TODO exit and dealloc memory
    }
}

void Scheduler::sigprocmask_unblock() {
    if (sigprocmask(SIG_UNBLOCK, &signals, nullptr) < 0) {
        std::cerr << SYSTEM_ERROR << "sigprocmask  failed" << std::endl;
        // TODO exit and dealloc memory
    }
}