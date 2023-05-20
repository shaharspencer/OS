#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#include <pthread.h>
#include <atomic>
#include <set>
#include <algorithm>
#include <semaphore.h>
#include <iostream>

#define SYSTEM_FAILURE_MESSAGE "system error: "
#define SYSTEM_FAILURE_EXIT 1

// TODO load function can fail; check erors

using namespace std;
typedef struct JobContext JobContext;

// todo add errors
// todo add percentages, stages etc
typedef struct ThreadContext {
    int threadID;
    /* grant access to mutual input and output vectors */
    const InputVec *inputVec;
    OutputVec *outputVec;
    /* unique intermediate vector */
    IntermediateVec *intermediateVec;
    /* grant access to mutual atomic counter, mutex, semaphore and barrier */
    atomic<uint64_t> *atomicCounter;
    pthread_mutex_t *mutex;
    sem_t *semaphore;
    Barrier *barrier;
    const MapReduceClient *client;
    JobContext * jobContext;
    vector<IntermediateVec>* shuffledOutput;
} ThreadContext;

// todo static casting
struct JobContext {
    pthread_t *threads;
    ThreadContext *contexts;
    atomic<uint64_t> *atomicCounter;
    pthread_mutex_t *mutex;
    sem_t *semaphore;
    Barrier *barrier;
};


void worker(void *arg);

vector<IntermediateVec>* shuffle(JobContext *tc);

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
    // TODO check memory allocation
    std::atomic<uint64_t> atomicCounter(0);
    /* initialize semaphore and mutex */

    pthread_mutex_t mtx;
    int res = pthread_mutex_init(&mtx, nullptr);
    if (res != 0){
        std::cout<<SYSTEM_FAILURE_MESSAGE<<"mutex allocation failed"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }

    sem_t sem;
    res = sem_init(&sem, 1, 1);
    if (res != 0){
        std::cout<<SYSTEM_FAILURE_MESSAGE<<"semaphore allocation failed"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }

    Barrier* barrier = new Barrier(multiThreadLevel);
    JobContext * jobContext = new JobContext;
    if (!jobContext){

        std::cout<<SYSTEM_FAILURE_MESSAGE<<"failed to allocate jobContext"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }
    *jobContext = (JobContext) {threads, contexts, &atomicCounter,
                                           &mtx, &sem, barrier};

    for (int i = 0; i < multiThreadLevel; i++) {
        ThreadContext *threadContext = new ThreadContext;
        if (!threadContext) {/*error*/ }
        *threadContext = (ThreadContext) {i, &inputVec, &outputVec, nullptr,
                          &atomicCounter, &mtx, &sem, barrier, &client,
                          nullptr, nullptr};

        if (i == 0) {
            threadContext->jobContext = jobContext;
        }

        contexts[i] = *threadContext;

    }

    for (int i = 0; i < multiThreadLevel; i++) {

        int result = pthread_create(threads + i, nullptr, reinterpret_cast<void *(*)(void *)>(worker), contexts + i);
        if (result != 0){
            std::cout << SYSTEM_FAILURE_MESSAGE<< "pthread_create function failed"<<std::endl;
            exit(SYSTEM_FAILURE_EXIT);
        }
    }


    return static_cast<JobHandle> (jobContext);
    // TODO figure out JobHandle
}



/**
 *
 */
void worker(void *arg) {
    ThreadContext *tc = (ThreadContext *) arg;

    /* MAP PHASE */

    /* main thread updates job state */
    if (tc->threadID == 0) {
        /* currently stage is UNDEFINED, change it to MAP */
        (*(tc->atomicCounter)) += 1ULL << 62;
        /* initialize number of keys to process */
        uint64_t keysNum = tc->inputVec->size();
        (*(tc->atomicCounter)) += keysNum << 31;
    }
    /* threads wait for main thread to finish updating atomicCounter */
    tc->barrier->barrier();

    /* define unique intermediate vector for map phase */
    tc->intermediateVec = new IntermediateVec(); // TODO  maybe should happen in emit2
    if (tc->intermediateVec == nullptr) {
        cout << SYSTEM_FAILURE_MESSAGE <<
            "intermidiate vector memory allocation failed" << std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }
    int x = 0;
    while (true) {
        uint64_t state = (*tc->atomicCounter) += 1ULL;
        uint64_t keysNum = state >> 31 & (0x7fffffff);
        uint64_t keysProcessed = state & (0x7fffffff);
        /* if all keys are processed, move on */
        if (keysProcessed >= keysNum) {
            break;
        }
        /* map chosen InputPair */
        K1 *key = tc->inputVec->at(keysProcessed).first;
        V1 *value = tc->inputVec->at(keysProcessed).second;
        tc->client->map(key, value, tc);
    }

    /* SORT PHASE */

    sort(tc->intermediateVec->begin(), tc->intermediateVec->end()); // TODO
    std::cout<<tc->intermediateVec<<std::endl;
    // add comparator for keys rather than comparing pairs
    /* wait for other thread to finish sorting */
    tc->barrier->barrier();
//
//    /* SHUFFLE PHASE */
//
//    //TODO mutex to all other threads
//
//    /* main thread updates job state and shuffles */
//    if (tc->threadID == 0) {
//        /* currently stage is MAP, change it to SHUFFLE */
//        uint64_t addition = 1ULL << 62;
//        (*tc->atomicCounter) += addition;
//        /* since number of intermediate pairs is the same as the number
//         * of input keys, the value of pairs to shuffle remains unchanged,
//         * and we only need to nullify number of shuffled pairs */
//        addition = ~(0xffffffffULL);
//        tc->atomicCounter+=addition;
//        /* do the shuffle */
//        vector<IntermediateVec>* shuffled = shuffle(tc->jobContext); // TODO change name
//        tc->shuffledOutput = shuffled;
//    }
//    /* threads wait for main thread to finish updating atomicCounter */
//    tc->barrier->barrier();
//
//    /* REDUCE PHASE */
//
//    vector<IntermediateVec> * shuffledVector = tc->shuffledOutput;
//
//    while (true)//TODO the remaining elements of the shuffled vector > 0)
//    {
//        IntermediateVec currVector = shuffledVector->back(); // get last element
//        shuffledVector->pop_back(); // remove last element
//        //TODO make sure no one else tries to take this element via mutex
//        // TODO lower the remaining count
//        tc->client->reduce(&currVector, tc);
//    }
}


void waitForJob(JobHandle job) {
    // TODO use pthread_join here
}

void getJobState(JobHandle job, JobState *state) {
    JobContext *jc = (JobContext *) job;
    uint64_t a = jc->atomicCounter->load();
    uint64_t stage = a >> 62 & (0x3ULL);
    uint64_t total = a >> 31 & (0x7fffffffULL);
    uint64_t processed = a & (0x7fffffffULL);
    *state = {stage_t(stage), float(processed) / float(total)};
}

void closeJobHandle(JobHandle job) {

}

// TODO make this FABULOUS



vector<IntermediateVec>* shuffle(JobContext *jc) {
//    bool flag = true;
//    vector<IntermediateVec>* shuffleOutput = new std::vector<IntermediateVec>();
//    while(true) {
//        std::set<K2>* max_potential_elements = new std::set<K2>();
//        if (max_potential_elements == nullptr)
//        {
//            cout << SYSTEM_FAILURE_MESSAGE << "max_potential_elements memory allocation failed"<<std::endl;
//            exit(SYSTEM_FAILURE_EXIT);
//        }
//        for (ThreadContext tc: jc->contexts)
//        {
//            if(!tc.intermediateVec->empty()) // TODO check
//            {
//                K2 elem = tc.intermediateVec->end()->first;
//                max_potential_elements->insert(elem);
//            }
//        }
//
//        if (max_potential_elements->empty()){
//            break;
//        }
//        const void* maxElem = (void *) &(std::max_element(max_potential_elements->begin(), max_potential_elements->end()));
//
//        IntermediateVec* newIntermidiateVector = new IntermediateVec();
//        if(newIntermidiateVector == nullptr){
//            cout << SYSTEM_FAILURE_MESSAGE << "intermediate vector memory allocation failed"<<std::endl;
//            exit(SYSTEM_FAILURE_EXIT);
//        }
//        for (ThreadContext tc: jc->contexts){
//            if((!tc.intermediateVec->empty()) && tc.intermediateVec->end()->first == maxElem) // TODO check
//            {
//                IntermediatePair elem = tc.intermediateVec->back();
//                tc.intermediateVec->pop_back();
//                newIntermidiateVector->push_back(elem);
//            }
//        }
//        shuffleOutput->push_back(newIntermidiateVector);
//    }

    return nullptr;
}