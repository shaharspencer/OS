#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>

using namespace std;

// todo static casting
typedef struct JobContext{
    //threads
    pthread_t* threads;
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
    InputVec* inputVec;
    OutputVec* outputVec;
    /* unique intermediate vector */
    IntermediateVec intermediateVec;
    //atomic int that represents which elemnt of the input vector we should proccess next
    std::atomic<int>* curr_input;
    Barrier* barrier;
    MapReduceClient& client;
    JobContext* jobContext;
} ThreadContext;

ThreadContext *createThreadContext(int threadID, InputVec *inputVec, OutputVec *outputVec,
                                   std::atomic<int> *atomic_counter, Barrier *barrier,
                                   MapReduceClient &client);

void single_thread_run(void* tc);

void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
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
void single_thread_run(void* tc){

    ThreadContext* threadContext = (ThreadContext*) tc;

    /* MAP PHASE */

    // define intermidiary vector for map phase
    threadContext->intermediateVec = new IntermediateVec(); // TODO memory TODO maybe should happen in emit2
    // check if map phase is done; else, contribute to map phase
    while (*(threadContext->curr_input) < threadContext->inputVec.size()){
        // save old value and increment
        int old_value = (*(threadContext->curr_input))++;
        // do map to old value
        K1* key = threadContext->inputVec.at(old_value).first;
        V1* value = threadContext->inputVec.at(old_value).second;
        threadContext->client.map(key, value, intermediateVec);
    }

    /* SORT PHASE */
    std::sort(intermediateVec->begin(), intermediateVec->end());
    threadContext->barrier->barrier();

    /* SHUFFLE PHASE */
    if (threadContext->threadID == 0) {

    }
    threadContext->barrier->barrier();
}




void waitForJob(JobHandle job){
    // TODO use pthread_join here
}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}
