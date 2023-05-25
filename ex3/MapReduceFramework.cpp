#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#include <pthread.h>
#include <atomic>
#include <set>
#include <algorithm>
#include <semaphore.h>
#include <iostream>
#include <unistd.h>

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

    pthread_t* threads = new pthread_t[multiThreadLevel];
    ThreadContext* contexts = new ThreadContext[multiThreadLevel];
    // TODO check memory allocation
    std::atomic<uint64_t> atomicCounter(uint64_t (0));
    /* initialize semaphore and mutex */

    auto* mtx = new pthread_mutex_t (PTHREAD_MUTEX_INITIALIZER);

    auto* barrier = new Barrier(multiThreadLevel);


    auto * jobContext = new JobContext();

    if (!jobContext){

        std::cout<<SYSTEM_FAILURE_MESSAGE<<"failed to allocate jobContext"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }
    *jobContext = (JobContext) {threads, contexts, &atomicCounter,
                                           mtx,
                                           barrier, multiThreadLevel, nullptr};

    for (size_t i = 0; i < (size_t) multiThreadLevel; i++) {
        auto *threadContext = new ThreadContext;
        if (!threadContext) {/*error*/ } //TODO
        *threadContext = (ThreadContext) {static_cast<int>(i), &inputVec, &outputVec, nullptr,
                          &atomicCounter, mtx, barrier, &client,
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

void updateJobState(ThreadContext* tc){

    Barrier* barrier = tc->barrier;
    const MapReduceClient * client = tc->client;
    const InputVec* inputVec = tc->inputVec;
    pthread_mutex_t* mutex = tc->mutex;
    /* currently stage is UNDEFINED, change it to MAP */
    (*(tc->atomicCounter)) += (1UL << 62);
    auto toPrint = tc->atomicCounter->load() >> 62 & (0x3);
    printf("Stage during beggin of worker in thread %d is %lu\n", tc->threadID, toPrint);

    /* initialize number of keys to process */
//    if (pthread_mutex_lock(mutex) != 0) {
//        printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
//        exit(SYSTEM_FAILURE_EXIT);
//    }
    auto keysNum = tc->inputVec->size();
//    if (pthread_mutex_unlock(mutex) != 0) {
//        printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
//        exit(SYSTEM_FAILURE_EXIT);
//    }
    printf("Number of keys: %lu\n", keysNum);
    (*(tc->atomicCounter)) += keysNum << 31;
//    tc->atomicCounter->fetch_add((uint64_t(keysNum) << 31));
//    std::atomic<uint64_t>& counterRef = *(tc->atomicCounter);

//// Clear bits 2-33
//    counterRef &= ~0x1FFFFFFFFCULL;
//
//// Set the desired bits to represent the number 3
//    counterRef |= 3ULL << 2;
    toPrint = tc->atomicCounter->load() >> 31 & (0x7fffffff);
    printf("Atomic counter after update to keysNum: %lu\n", toPrint);
//    printf("finished updating atomic counter\n");
}


void defineIntermidiateVector(ThreadContext* tc){
    //    printf("Will now try to allocate intermediate vector on thread %d\n", tc->threadID);
    auto *intermediateVec = new IntermediateVec ();
    if (intermediateVec == nullptr) {
        printf ("%s intermediate vector memory allocation failed,\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
    printf("Defined intermidiate vec at address %p for thread %d\n",
           intermediateVec, tc->threadID);
    tc->intermediateVec = intermediateVec;
}

void mapForThread(ThreadContext* tc){
        printf("map phase in thread %d\n", tc->threadID);
        Barrier* barrier = tc->barrier;
        const MapReduceClient * client = tc->client;
        const InputVec* inputVec = tc->inputVec;
        pthread_mutex_t* mutex = tc->mutex;
        while (true) {
            (*(tc->atomicCounter))++;
        auto keysProcessed = (*(tc->atomicCounter)).load() & (0x7fffffff);
        auto keysNum = (*(tc->atomicCounter)).load() >> 31 & (0x7fffffff);
        /* if all keys are processed, move on */
        if (keysProcessed >= keysNum) {
            printf("processed %lu out of %lu hence stops\n", keysProcessed, keysNum);
            break;
        }
        printf("processed %lu out of %lu hence continues\n", keysProcessed, keysNum);

//        if (pthread_mutex_lock(mutex) != 0) {
//            printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
//            exit(SYSTEM_FAILURE_EXIT);
//        }
        /* map chosen InputPair */
        K1 *key = inputVec->at(keysProcessed).first;
        V1 *value = inputVec->at(keysProcessed).second;
        client->map(key, value, tc);
//        if (pthread_mutex_unlock(mutex) != 0) {
//            printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
//            exit(SYSTEM_FAILURE_EXIT);
//        }
    }
    printf("finished map in thread %d\n", tc->threadID);

}

void reduceForThread(ThreadContext* tc){
        printf("reducing in thread\n");
        while (true) {
        uint64_t state = ((tc->jobContext->atomicCounter->load()));
        uint64_t keysNum = state >> 31 & (0x7fffffff);
        uint64_t keysProcessed = state & (0x7fffffff);
        /* if all keys are processed, move on */
        if (keysProcessed >= keysNum) {
            break;
        }
        /* pop an IntermediateVec from back of shuffledOutput */
        if (pthread_mutex_lock(tc->mutex) == 0) {
            printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
            exit(SYSTEM_FAILURE_EXIT);
        }
        if (tc->jobContext->shuffledOutput->empty()) {
            ::printf("error: shuffledOutput should not be empty!!!!!!!!!!!\n");
        }

        IntermediateVec intermediateVec = tc->jobContext->shuffledOutput->back();
        tc->jobContext->shuffledOutput->pop_back();
        if (pthread_mutex_unlock(tc->mutex) == 0) {
            printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
            exit(SYSTEM_FAILURE_EXIT);
        }
        /* reduce chosen IntermediateVec */
        tc->client->reduce(&intermediateVec, tc);
        *(tc->jobContext->atomicCounter) += uint64_t(intermediateVec.size());
    }
    printf("finished reduce\n");

}

void shufflePhase(ThreadContext* tc){
    /* main thread updates job state and shuffles */
    if (tc->threadID == 0) {
        /* currently stage is MAP, change it to SHUFFLE */
        uint64_t addition = 1UL << 62;
        (*tc->atomicCounter) += addition;
        /* since number of intermediate pairs is the same as the number
         * of input keys, the value of pairs to shuffle remains unchanged,
         * and we only need to nullify number of shuffled pairs */
        addition = ~(0x7fffffff);
        tc->atomicCounter+=addition;
        /* do the shuffle */
        shuffle(tc->jobContext);
    }
}
/**
 *
 */
void worker(void *arg) {
    /* define arguments for worker */
    auto *tc = (ThreadContext *) arg;
    Barrier* barrier = tc->barrier;
    const MapReduceClient * client = tc->client;
    const InputVec* inputVec = tc->inputVec;
    pthread_mutex_t* mutex = tc->mutex;

    printf("beggining worker in thread %d\n", tc->threadID);

    /* MAP PHASE */

    /* main thread updates job state */
    if (tc->threadID == 0) {
        updateJobState(tc);
    }
    /* threads wait for main thread to finish updating atomicCounter */
    tc->barrier->barrier();

    /* define unique intermediate vector for map phase */
    defineIntermidiateVector(tc);
    barrier->barrier();

    /* MAP PHASE */
    mapForThread(tc);

    /* SORT PHASE */
    printf("sorting vector\n");
    sort(tc->intermediateVec->begin(),
         tc->intermediateVec->end(), compare);


    /* wait for other thread to finish sorting */
    tc->jobContext->barrier->barrier();

    /* SHUFFLE PHASE */
    if (tc->threadID == 0){
        shufflePhase(tc);
    }

    /* threads wait for main thread to finish updating atomicCounter */
    tc->jobContext->barrier->barrier();

//    /* REDUCE PHASE */
//    /* main thread updates job state */
    if (tc->threadID == 0) {
        printf("Thread 0 will now attempt atomic counter changes\n");
        /* currently stage is SHUFFLE, change it to REDUCE */
        auto temp = (*(tc->jobContext->atomicCounter)).load();
        printf("loaded\n");
        uint64_t addition = 1UL << 62;
        (*(tc->jobContext->atomicCounter)) = temp + addition;
        printf("ok\n");
        /* nullify number of processed keys */
        (*(tc->jobContext->atomicCounter)) &= ~(0x7fffffff);
        printf("finished\n");
    }
//    /* threads wait for main thread to finish updatin
//     * g atomicCounter */
    tc->jobContext->barrier->barrier();
    reduceForThread(tc);
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

void getJobState(JobHandle job, JobState *state) { //TODO CHANGES ATOMIC VARIABLE!!!!!!!!!
    if (pthread_mutex_lock(mutex) != 0) {
        printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
    auto a = jc->atomicCounter->load();
    auto stage = jc->atomicCounter->load() >> 62 & (0x3);
    auto total = jc->atomicCounter->load() >> 31 & (0x7fffffff);
    auto processed = jc->atomicCounter->load() & (0x7fffffff);
    *state = {stage_t(stage), float(processed) / float(total) * 100};
    if (pthread_mutex_unlock(mutex) != 0) {
        printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
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
    printf("finished shuffling\n");
    jc->shuffledOutput = shuffleOutput;
}


bool compare (IntermediatePair p1, IntermediatePair p2) {
    return *p1.first < *p2.first;
}