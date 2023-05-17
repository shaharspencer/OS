#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

#include <pthread.h>
#include <semaphore.h>
#include <algorithm>
#include <atomic>
#include <set>

using namespace std;

// todo add errors
// todo add percentages, stages etc
typedef struct ThreadContext {
    int threadID;
    /* grant access to mutual input and output vectors */
    InputVec *inputVec;
    OutputVec *outputVec;
    /* unique intermediate vector */
    IntermediateVec *intermediateVec;
    /* grant access to mutual atomic counter, mutex, semaphore and barrier */
    atomic<uint64_t> *atomicCounter;
    pthread_mutex_t *mutex;
    sem_t *semaphore;
    Barrier *barrier;
    MapReduceClient *client;
} ThreadContext;

// todo static casting
typedef struct JobContext {
    pthread_t *threads;
    ThreadContext *contexts;
    atomic<uint64_t> *atomicCounter;
    pthread_mutex_t *mutex;
    sem_t *semaphore;
    Barrier *barrier;
} JobContext;


void worker(void *arg);

vector<IntermediateVec>* shuffle(JobContext *tc);

ThreadContext *createThreadContext(int threadID, InputVec *inputVec, OutputVec *outputVec,
                                   IntermediateVec *intermediateVec,
                                   std::atomic<int> *atomic_counter, Barrier *barrier,
                                   MapReduceClient &client, JobContext *jobContext);

void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->intermediateVec->push_back({key, value});
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->outputVec->push_back({key, value});

}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    std::atomic<uint64_t> atomicCounter(0);
    pthread_mutex_t* mutex;
    sem_t* semaphore;
    Barrier barrier(multiThreadLevel);
    JobContext *jobContext = (JobContext) {&threads, &contexts, &atomicCounter,
                                           &mutex, &semaphore, &barrier};

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i] = createThreadContext(i, inputVec, outputVec, nullptr,
                                          &atomicCounter, &barrier, client, jobContext);
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(threads + i, nullptr, worker, contexts + i);
    }


    return static_cast<JobHandle> (jobContext);
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
void worker(void *arg) {
    ThreadContext *tc = (ThreadContext *) arg;

    /* MAP PHASE */
    /* main thread updates job state */
    if (tc->threadID == 0) {
        (*(tc->atomicCounter)) += 1 << 62;
    }

    /* define unique intermediate vector for map phase */
    tc->intermediateVec = new IntermediateVec(); // TODO memory TODO maybe should happen in emit2
    /* check if map phase is done; else, contribute to map phase */
    while (*(tc->curr_input) < tc->inputVec->size()) {
        /* save old value and increment */
        int old_value = (*(tc->curr_input))++; // TODO make sure this agrees with 64bit implementation
        /* do map to old value */
        K1 *key = tc->inputVec->at(old_value).first;
        V1 *value = tc->inputVec->at(old_value).second;
        tc->client.map(key, value, tc);
    }

    /* SORT PHASE */
    std::sort(tc->intermediateVec->begin(), tc->intermediateVec->end());
    tc->barrier->barrier();

    /* SHUFFLE PHASE */
    //TODO mutex to all other threads
    if (tc->threadID == 0) {
        vector<IntermediateVec>* shuffled = shuffle(tc->jobContext); // TODO change name
        tc->shuffleOutput = shuffled;
    }
    tc->barrier->barrier();

    /* REDUCE PHASE */
    vector<IntermediateVec> * shuffledVector = tc->shuffleOutput;

    while (true)//TODO the remaining elements of the shuffled vector > 0)
    {
        IntermediateVec currVector = shuffledVector->back(); // get last element
        shuffledVector->pop_back(); // remove last element
        //TODO make sure no one else tries to take this element via mutex
        // TODO lower the remaining count
        tc->client.reduce(&currVector, tc);
    }
}


void waitForJob(JobHandle job) {
    // TODO use pthread_join here
}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {

}

// TODO make this FABULOUS
void initializeAtomicCounter(std::atomic<uint64_t>* atomicCounter, AtomicCounterBitsRange atomicCounterBitsRange) {
    BitsRange bitsRange;
    switch (atomicCounterBitsRange) {
        case STAGE:
            bitsRange = {62, 2};
            break;
        case PROCESSED_KEYS:
            bitsRange = {31, 31};
            break;
        case KEYS_TO_PROCESS:
            bitsRange = {0, 31};
            break;
        default:
            // TODO error
            return;
    }
    uint64_t mask = ((1ULL << bitsRange.length) - 1) << bitsRange.start;
    *atomicCounter = *atomicCounter & ~mask;
    // TODO make this so it can change desired bits to given value
}

void incrementAtomicCounter(std::atomic<uint64_t>* atomicCounter, AtomicBitType bitType);

vector<IntermediateVec>* shuffle(JobContext *jc) {
    bool flag = true;
    vector<IntermediateVec> shuffleOutput;
    while(true) {
        std::set<K2>* max_potential_elements = new std::set<K2>();

        for (ThreadContext tc: jc->contexts)
        {
            if(!tc.intermediateVec->empty()) // TODO check
            {
                K2 elem = tc.intermediateVec->end()->first;
                max_potential_elements->insert(elem);
            }
        }

        if (max_potential_elements->empty()){
            break;
        }

        K2 maxElem = (std::max_element(max_potential_elements->begin(), max_potential_elements->end()));

        IntermediateVec newIntermidiateVector = new IntermediateVec();
        for (ThreadContext tc: jc->contexts){
            if((!tc.intermediateVec->empty()) && tc.intermediateVec->end()->first == maxElem) // TODO check
            {
                IntermediatePair elem = tc.intermediateVec->pop_back();
                newIntermidiateVector.push_back(elem)
            }
        }
        shuffleOutput.push_back(newIntermidiateVector);
    }
    return &shuffleOutput;
}