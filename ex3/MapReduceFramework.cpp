#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#include <pthread.h>
#include <atomic>
#include <set>
#include <algorithm>
#include <semaphore.h>
#include <iostream>

#define SYSTEM_FAILURE_MESSAGE "system error:"
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
    JobContext *jobContext;

} ThreadContext;

// todo static casting
struct JobContext {
    pthread_t *threads;
    ThreadContext *contexts;
    atomic<uint64_t> *atomicCounter;
    pthread_mutex_t *mutex;
    sem_t *semaphore;
    Barrier *barrier;
    int multiThreadLevel;
    vector<IntermediateVec>* shuffledOutput;
};


void worker(void *arg);
bool compare(IntermediatePair p1, IntermediatePair p2);

void shuffle(JobContext *jc);

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
    res = sem_init(&sem, 0, 1);
    if (res != 0){
        std::cout<<SYSTEM_FAILURE_MESSAGE<<"semaphore allocation failed"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }

    Barrier* barrier = new Barrier(multiThreadLevel);
    auto * jobContext = new JobContext();
    if (!jobContext){

        std::cout<<SYSTEM_FAILURE_MESSAGE<<"failed to allocate jobContext"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }
    *jobContext = (JobContext) {threads, contexts, &atomicCounter,
                                           &mtx, &sem,
                                           barrier, multiThreadLevel, nullptr};

    for (int i = 0; i < multiThreadLevel; i++) {
        auto *threadContext = new ThreadContext;
        if (!threadContext) {/*error*/ }
        *threadContext = (ThreadContext) {i, &inputVec, &outputVec, nullptr,
                          &atomicCounter, &mtx, &sem, barrier, &client,
                          jobContext};

        contexts[i] = *threadContext;
    }

    for (int i = 0; i < multiThreadLevel; i++) {

        int result = pthread_create(threads + i,
                                    nullptr, reinterpret_cast<void *(*)(void *)>(worker),
                                    contexts + i);
        if (result != 0){
            std::cout << SYSTEM_FAILURE_MESSAGE<< "pthread_create function failed"<<std::endl;
            exit(SYSTEM_FAILURE_EXIT);
        }
    }
    return static_cast<JobHandle> (jobContext);

}

/**
 *
 */
void worker(void *arg) {

    auto *tc = (ThreadContext *) arg;

    /* MAP PHASE */

    /* main thread updates job state */
    if (tc->threadID == 0) {

        /* currently stage is UNDEFINED, change it to MAP */
        (*(tc->atomicCounter)) += 1ULL << 62;
        uint64_t toPrint = tc->atomicCounter->load()>> 62 & (0x3ULL);
//        printf("Stage during beggin of worker in thread %d is %llu\n", tc->threadID, toPrint);

        /* initialize number of keys to process */
        uint64_t keysNum = tc->inputVec->size();
//        printf("Number of keys: %llu \n", keysNum);
        (*(tc->atomicCounter)) += keysNum << 31;
//        toPrint = tc->atomicCounter->load() >> 31 & (0x7fffffffULL);
//        printf("Atomic counter after update to keysNum: %llu\n", toPrint);
    }
    /* threads wait for main thread to finish updating atomicCounter */
//    tc->barrier->barrier();

    /* define unique intermediate vector for map phase */
    tc->intermediateVec = new IntermediateVec();
    if (tc->intermediateVec == nullptr) {
        printf ("%s intermediate vector memory allocation failed,\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
    tc->jobContext->barrier->barrier();
////    printf("Defined intermidiate vec at address %p for thread %d\n",
////             tc->intermediateVec, tc->threadID);
////    tc->barrier->barrier();
//
//
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
//
    /* SORT PHASE */

    sort(tc->intermediateVec->begin(),
         tc->intermediateVec->end(), compare);

    tc->jobContext->barrier->barrier();

    /* wait for other thread to finish sorting */
    tc->jobContext->barrier->barrier();

    /* SHUFFLE PHASE */

    /* main thread updates job state and shuffles */
    if (tc->threadID == 0) {
        /* currently stage is MAP, change it to SHUFFLE */
        uint64_t addition = 1ULL << 62;
        (*tc->atomicCounter) += addition;
        /* since number of intermediate pairs is the same as the number
         * of input keys, the value of pairs to shuffle remains unchanged,
         * and we only need to nullify number of shuffled pairs */
        addition = ~(0x7fffffffULL);
        tc->atomicCounter+=addition;
        /* do the shuffle */
        shuffle(tc->jobContext);
    }
    /* threads wait for main thread to finish updating atomicCounter */
    tc->jobContext->barrier->barrier();

    /* REDUCE PHASE */
    /* main thread updates job state */
    if (tc->threadID == 0) {
        /* currently stage is SHUFFLE, change it to REDUCE */
        (*(tc->atomicCounter)) += 1ULL << 62;
        /* nullify number of processed keys */
        (*(tc->atomicCounter)) &= ~(0x7fffffffULL);
    }
    /* threads wait for main thread to finish updatin
     * g atomicCounter */
    tc->jobContext->barrier->barrier();

//    while (true) {
//        uint64_t state = ((tc->atomicCounter->load()));
//        uint64_t keysNum = state >> 31 & (0x7fffffff);
//        uint64_t keysProcessed = state & (0x7fffffff);
//        /* if all keys are processed, move on */
//        if (keysProcessed >= keysNum) {
//            break;
//        }
//        /* pop an IntermediateVec from back of shuffledOutput */
//        if (pthread_mutex_lock(tc->mutex) == 0) {
//            printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
//            exit(SYSTEM_FAILURE_EXIT);
//        }
//        if (tc->jobContext->shuffledOutput->empty()) {
//            ::printf("error: shuffledOutput should not be empty!!!!!!!!!!!\n");
//        }
//
//        IntermediateVec intermediateVec = tc->jobContext->shuffledOutput->back();
//        tc->jobContext->shuffledOutput->pop_back();
//        if (pthread_mutex_unlock(tc->mutex) == 0) {
//            printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
//            exit(SYSTEM_FAILURE_EXIT);
//        }
//        /* reduce chosen IntermediateVec */
//        tc->client->reduce(&intermediateVec, tc);
//        *(tc->atomicCounter) += uint64_t(intermediateVec.size());
//    }
}


void waitForJob(JobHandle job) {
    auto * jc = (JobContext* ) job;
    for (int i = 0; i < jc->multiThreadLevel; i++){
        if (pthread_join(jc->threads[i], nullptr)){
            std::cout << SYSTEM_FAILURE_MESSAGE << "pthread_join failed"<<std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    JobContext *jc = (JobContext *) job;
    uint64_t a = jc->atomicCounter->load();
    uint64_t stage = a >> 62 & (0x3ULL);
    uint64_t total = a >> 31 & (0x7fffffffULL);
    uint64_t processed = a & (0x7fffffffULL);
    *state = {stage_t(stage), float(processed) / float(total) * 100};
}

void closeJobHandle(JobHandle job) {
    //TODO delete all memory
}

IntermediatePair getMaxElement(JobContext* jc){
    IntermediatePair pair = IntermediatePair ({NULL, NULL});
    /* iterate over threads */
    for (size_t i = 0; i < jc->multiThreadLevel; i++){
       if (!(jc->contexts[i].intermediateVec->empty())){
           IntermediatePair currElement = jc->contexts[i].intermediateVec->back();
           if ((!pair.first) || compare(pair, currElement)){
               pair = currElement;
           }
       }
    }
    return pair;
}


void shuffle(JobContext *jc) {
    ::printf("in shuffle function\n");
//    printf("printing sorted vectors\n");
//    for (size_t i = 0; i < jc->multiThreadLevel; i++){
//        ::printf(" in thread %zu: \n", i);
//
//        jc->contexts[0].client->reduce(jc->contexts[i].intermediateVec, nullptr);
//    }
//    ::printf("begin shuffle phase\n");
    auto shuffleOutput = new std::vector<IntermediateVec>();

    while(true) {
        /* get current max element */
        IntermediatePair maxElement = getMaxElement(jc);
        /* if there is no max element, we are finished */
        if (!maxElement.first) {
            break;
        }
        /* create vector of elements that are the same as max element*/
        auto newIntermidiateVector = new IntermediateVec();
        for (size_t i = 0; i < jc->multiThreadLevel; i++) {
            /*  while this vector has more elements and the
             * last element in this vector has max value */
            while   (
                    !jc->contexts[i].intermediateVec->empty()
                    &&
                    !(compare(jc->contexts[i].intermediateVec->back(), maxElement))
                    &&
                    !(compare(maxElement, jc->contexts[i].intermediateVec->back()))
                    )
                {
                    IntermediatePair currElement = jc->contexts[i].intermediateVec->back();
                    /* add to the key's intermidiate vector */
                    newIntermidiateVector->push_back(currElement);
                    /* take off the back of the current vector */
                    jc->contexts[i].intermediateVec->pop_back();
                }
            }
//        ::printf("max element vector is: ");
//        jc->contexts[0].client->reduce(newIntermidiateVector, nullptr);
        shuffleOutput->push_back(*newIntermidiateVector);
    }
    jc->shuffledOutput = shuffleOutput;
}


bool compare (IntermediatePair p1, IntermediatePair p2) {
    return *p1.first < *p2.first;
}