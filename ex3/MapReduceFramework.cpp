#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>

using namespace std;

// todo static casting
typedef struct JobContext {
    //threads
    pthread_t *threads;
    //state
    stage_t state;
    //mutexes TODO

    //
} JobContext;

// todo add errors
// todo add percentages, stages etc
typedef struct ThreadContext {
    int threadID;
    /* grant access to mutual input and output vectors */
    InputVec *inputVec;
    OutputVec *outputVec;
    /* unique intermediate vector */
    IntermediateVec *intermediateVec;
    /* grant access to mutual atomic counter and barrier */
    std::atomic<int> *curr_input;
    Barrier *barrier;
    MapReduceClient &client;
    JobContext *jobContext;
} ThreadContext;

ThreadContext *createThreadContext(int threadID, InputVec *inputVec, OutputVec *outputVec,
                                   std::atomic<int> *atomic_counter, Barrier *barrier,
                                   MapReduceClient &client);

void single_thread_run(void *arg);

void emit2(K2 *key, V2 *value, void *context) {

}

void emit3(K3 *key, V3 *value, void *context) {

}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    std::atomic<int> atomic_counter(0);
    Barrier barrier(multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i] = createThreadContext(i, inputVec, outputVec, nullptr, &atomic_counter, &barrier, client);
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(threads + i, nullptr, single_thread_run, contexts + i);
    }

    // TODO figure out JobHandle
}

ThreadContext *createThreadContext(int threadID, InputVec *inputVec, OutputVec *outputVec,
                                   IntermediateVec intermediateVec,
                                   std::atomic<int> *atomic_counter, Barrier *barrier,
                                   MapReduceClient &client, JobContext *jobContext) {
    ThreadContext *threadContext = {threadID, inputVec, outputVec, intermediateVec,
                                    atomic_counter, barrier, client, nullptr};
    if (threadID == 0) {
        threadContext->jobContext = jobContext;
    }
    return threadContext;
}

/**
 *
 */
void single_thread_run(void *arg) {

    ThreadContext *tc = (ThreadContext *) arg;

    /* MAP PHASE */

    // define intermidiary vector for map phase
    tc->intermediateVec = new IntermediateVec(); // TODO memory TODO maybe should happen in emit2
    // check if map phase is done; else, contribute to map phase
    while (*(tc->curr_input) < tc->inputVec.size()) {
        // save old value and increment
        int old_value = (*(tc->curr_input))++;
        // do map to old value
        K1 *key = tc->inputVec.at(old_value).first;
        V1 *value = tc->inputVec.at(old_value).second;
        tc->client.map(key, value, tc);
    }

    /* SORT PHASE */
    std::sort(tc->intermediateVec->begin(), tc->intermediateVec->end());
    tc->barrier->barrier();

    /* SHUFFLE PHASE */
    if (tc->threadID == 0) {

    }
    tc->barrier->barrier();
}


void waitForJob(JobHandle job) {
    // TODO use pthread_join here
}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {

}
