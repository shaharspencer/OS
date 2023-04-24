#include "Thread.h"

#include <iostream>

#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
        "rol    $0x11,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5


/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr) {
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
                 "rol    $0x9,%0\n"
            : "=g" (ret)
            : "0" (addr));
    return ret;
}


#endif

// TODO handle main thread scenario
Thread::Thread(int tid, thread_entry_point entry_point) :
tid(tid), state(READY), sleeping_time(AWAKE), quanta_counter(0) {
    /* Allocates pseudo-stack for the new thread */
    stack = new char[STACK_SIZE];
    if(!stack) {
        std::cerr << "malloc error" << std::endl; // TODO use error macro
    }

    /* Initializes env, same as in demo_jmp.c */
    address_t sp = (address_t) stack + STACK_SIZE - sizeof(address_t);
    address_t pc = (address_t) entry_point;
    sigsetjmp(env, 1);
    (env->__jmpbuf)[JB_SP] = translate_address(sp);
    (env->__jmpbuf)[JB_PC] = translate_address(pc);
    sigemptyset(&env->__saved_mask);
}

Thread::~Thread() {
    delete[] stack;
}

int Thread::get_tid() {
    return tid;
}

State Thread::get_state() {
    return state;
}

void Thread::set_state(State s) {
    state = s;
}

int Thread::get_quanta_counter() {
    return quanta_counter;
}

void Thread::increment_quanta_counter() {
    quanta_counter++;
}

int Thread::thread_sigsetsetjmp() {
    int x = sigsetjmp(env, 1);
    if (x){
        throw std::system_error(errno, std::generic_category(),SYSTEM_ERROR + "sigsetjmp failed in thread\n");
    }
    return x;
}

void Thread::thread_siglongjmp(int val) {

    siglongjmp(env, val);
//        throw std::system_error(errno, std::generic_category(),SYSTEM_ERROR + "siglongjmp failed in thread\n");
//    }

}