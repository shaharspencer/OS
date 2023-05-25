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
#include <iostream>
#include <bitset>

#define SYSTEM_FAILURE_MESSAGE "system error:"
#define SYSTEM_FAILURE_EXIT 1

// TODO load function can fail; check erors

using namespace std;
typedef struct JobContext JobContext;

// todo add errors
// todo add percentages, stages etc
typedef struct ThreadContext {
    int threadID;
    /* unique intermediate vector */
    IntermediateVec *intermediateVec;
    /* grant access to mutual components */
    JobContext *jobContext;

} ThreadContext;

struct JobContext {
    pthread_t *threads;
    ThreadContext *contexts;
    /* grant access to mutual input and output vectors */
    const InputVec *inputVec;
    OutputVec *outputVec;
    stage_t *stage;
    uint32_t *total;
    atomic<uint32_t> *proccessed;
    const MapReduceClient *client;

    pthread_mutex_t *mutex;
    Barrier *barrier;
    int multiThreadLevel;
    vector<IntermediateVec>* shuffledOutput;
};


void worker(void *arg);
bool compare(IntermediatePair p1, IntermediatePair p2);

void shuffle(JobContext *jc);
//void updateJobState(ThreadContext* tc);

//void printBinary(unsigned long num) {
//    unsigned long firstSegment = num >> 62;
//    unsigned long secondSegment = (num >> 31) & 0x7FFFFFFF;
//    unsigned long thirdSegment = num & 0x7FFFFFFF;
//
//    std::bitset<2> binaryFirst(firstSegment);
//    std::bitset<31> binarySecond(secondSegment);
//    std::bitset<31> binaryThird(thirdSegment);
//
//    std::cout << binaryFirst <<std::endl<< binarySecond << std::endl << binaryThird << std::endl;
//}


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->intermediateVec->push_back({key, value});
    (*(tc->jobContext->proccessed))++;
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->jobContext->outputVec->push_back({key, value});
    (*(tc->jobContext->proccessed))++;
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    auto* threads = new pthread_t[multiThreadLevel];
    auto* contexts = new ThreadContext[multiThreadLevel];
    // TODO check memory allocation

    auto* stage = new stage_t(UNDEFINED_STAGE);
    auto* total = new uint32_t (0);
    auto* processed = new std::atomic<uint32_t>(0);
    /* initialize semaphore and mutex */

    auto* mtx = new pthread_mutex_t (PTHREAD_MUTEX_INITIALIZER);

    auto* barrier = new Barrier(multiThreadLevel);


    auto * jobContext = new JobContext();

    if (!jobContext){

        std::cout<<SYSTEM_FAILURE_MESSAGE<<"failed to allocate jobContext"<<std::endl;
        exit(SYSTEM_FAILURE_EXIT);
    }
    *jobContext = (JobContext) {threads, contexts, &inputVec,
                                &outputVec, stage, total, processed, &client,
                                mtx, barrier, multiThreadLevel, nullptr};

    for (int i = 0; i < multiThreadLevel; i++) {
        auto *threadContext = new ThreadContext;
        if (!threadContext) {/*error*/ } //TODO
        *threadContext = (ThreadContext) {i, nullptr,jobContext};
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

void unlock_mutex(JobContext* jobContext){
    if (pthread_mutex_unlock(jobContext->mutex) != 0) {
        printf("%s mutex unlock failed.\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
}

void lock_mutex(JobContext* jobContext){
    if (pthread_mutex_lock(jobContext->mutex) != 0) {
        printf("%s mutex lock failed.\n", SYSTEM_FAILURE_MESSAGE);
        exit(SYSTEM_FAILURE_EXIT);
    }
}

//void updateJobState(ThreadContext* tc){
//
//
//}


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
//        printf("map phase in thread %d\n", tc->threadID);
//        Barrier* barrier = tc->jobContext->barrier;
        const MapReduceClient * client = tc->jobContext->client;
        const InputVec* inputVec = tc->jobContext->inputVec;
        lock_mutex(tc->jobContext);
        auto keysNum = inputVec->size();
//        printf("keys num: %zu\n", keysNum);
        unlock_mutex(tc->jobContext);


        while (tc->jobContext->proccessed->load() < *(tc->jobContext->total)) {
        /* map chosen InputPair */
            lock_mutex(tc->jobContext);
            printf("processed %u out of %u hence continues\n", tc->jobContext->proccessed->load(), *(tc->jobContext->total));
            K1 *key = inputVec->at(tc->jobContext->proccessed->load()).first;
            V1 *value = inputVec->at(tc->jobContext->proccessed->load()).second;
            unlock_mutex(tc->jobContext);
            client->map(key, value, tc);

        }
    printf("finished map in thread %d\n", tc->threadID);

}

void reduceForThread(ThreadContext* tc){
    auto keysNum = tc->jobContext->shuffledOutput->size();
//        printf("reducing in thread\n");
    while (true) {
//            auto keysNum = *(tc->jobContext->total);
        lock_mutex(tc->jobContext);
        auto keysProcessed = tc->jobContext->proccessed->load();

        /* if all keys are processed, move on */
        if (keysProcessed >= keysNum) {
            break;
        }

        /* pop an IntermediateVec from back of shuffledOutput */

        if (tc->jobContext->shuffledOutput->empty()) {
            ::printf("error: shuffledOutput should not be empty!!!!!!!!!!!\n");
        }

        IntermediateVec intermediateVec = tc->jobContext->shuffledOutput->back();
        tc->jobContext->shuffledOutput->pop_back();

        /* reduce chosen IntermediateVec */
        tc->jobContext->client->reduce(&intermediateVec, tc);

        unlock_mutex(tc->jobContext);
    }
    printf("finished reduce\n");

}

void shufflePhase(ThreadContext* tc){
    /* main thread updates job state and shuffles */
    if (tc->threadID == 0) {
//        lock_mutex(tc->jobContext);
        /* currently stage is MAP, change it to SHUFFLE */
//        lock_mutex(tc->jobContext);
        *(tc->jobContext->stage) = SHUFFLE_STAGE;
//        unlock_mutex(tc->jobContext);
        /* update total to number of IntermediatePairs,
         * i.e. sum of IntermediateVectors sizes */
        unsigned int new_total = 0;
//        for (int i = 0; i < tc->jobContext->multiThreadLevel; i++) {
//            new_total += tc->jobContext->contexts[i].intermediateVec->size();
//        }
        *(tc->jobContext->total) = new_total;
         /* nullify number of shuffled pairs */
//        lock_mutex(tc->jobContext);
        *(tc->jobContext->proccessed) = 0;
//        unlock_mutex(tc->jobContext);
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

//    printf("beggining worker in thread %d\n", tc->threadID);

    /* MAP PHASE */

    /* main thread updates job state */
    if (tc->threadID == 0) {
        const InputVec* inputVec = tc->jobContext->inputVec;
        /* currently stage is UNDEFINED, change it to MAP */
//        lock_mutex(tc->jobContext);
        *(tc->jobContext->stage) = MAP_STAGE;
        printf("stage updated to: %u\n", *(tc->jobContext->stage));
//        unlock_mutex(tc->jobContext);

//        lock_mutex(tc->jobContext);
        *(tc->jobContext->total) = inputVec->size();
        printf("total updated to %u\n", *(tc->jobContext->total));
//        unlock_mutex(tc->jobContext);
//        lock_mutex(tc->jobContext);
        *(tc->jobContext->proccessed) = 0;
        printf("processed updated to %u\n", tc->jobContext->proccessed->load());
    }
    /* threads wait for main thread to finish updating atomicCounter */
    tc->jobContext->barrier->barrier();

    /* define unique intermediate vector for map phase */
    defineIntermidiateVector(tc);

    tc->jobContext->barrier->barrier();

    /* MAP PHASE */
    mapForThread(tc);

    /* SORT PHASE */
//    printf("sorting vector\n");
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

    /* REDUCE PHASE */
    /* main thread updates job state */
    if (tc->threadID == 0) {

        /* currently stage is SHUFFLE, change it to REDUCE */
        *(tc->jobContext->stage) = REDUCE_STAGE;
        *(tc->jobContext->total) = tc->jobContext->shuffledOutput->size();

    }
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


void getJobState(JobHandle jc, JobState *state) {
    auto *ctx = (JobContext *) jc;
    auto stage = *(ctx->stage);
    auto total = *(ctx->total);
    auto processed = ctx->proccessed->load();
    float processed_percent = (float(processed) / float(total)) * 100;
    *state = {stage_t(stage), processed_percent};
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
        printf("vector size is %zu\n", newIntermidiateVector->size());
    }
    printf("finished shuffling\n");
    printf("shuffled vector length is %zu\n", shuffleOutput->size());
    jc->shuffledOutput = shuffleOutput;

}


bool compare (IntermediatePair p1, IntermediatePair p2) {
    return *p1.first < *p2.first;
}