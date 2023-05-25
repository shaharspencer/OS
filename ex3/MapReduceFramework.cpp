#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#include <pthread.h> // for pthread_t, pthread_mutex_t
#include <atomic> // for atomic
#include <string>
#include <set>
#include <algorithm>
#include <iostream>
#include <unistd.h>

using namespace std;

#define SYSTEM_FAILURE_MESSAGE "system error:"
#define SYSTEM_FAILURE_EXIT 1

/*****************************************************************************/

typedef struct ThreadContext {
    int threadID;
    /* unique IntermediateVec for MAP and SORT phases */
    IntermediateVec *intermediateVec;
    /* grant access to shared components */
    struct JobContext *jobContext;
} ThreadContext;

typedef struct JobContext {
    /* client and components given by it */
    const MapReduceClient *client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    int multiThreadLevel;
    /* workers and their respective contexts */
    pthread_t *threads;
    ThreadContext *contexts;
    /* workers synchronization components */
    pthread_mutex_t *mutex;
    Barrier *barrier;
    /* components for keeping track of job progress */
    stage_t *stage;
    uint32_t *total;
    std::atomic<uint32_t> *processed;
    /* pointer to SHUFFLE phase output */
    std::vector<IntermediateVec> *shuffledOutput;
} JobContext;

/*****************************************************************************/

/* function for spawned threads to carry out MapReduce job */
void *worker (void *arg);

/* functions called by all workers for MAP and REDUCE phases */
void defineIntermediateVector (ThreadContext* tc);
void workerMap (ThreadContext *tc);
void workerSort (ThreadContext *tc);
void workerReduce (ThreadContext *tc);

/* compare function to be used by workerSort() during SORT phase
 * and by getMaxElement() during SHUFFLE phase */
bool compare (IntermediatePair p1, IntermediatePair p2);

/* functions called by worker 0 only, for stage progress and SHUFFLE phase */
void initMap (JobContext *jc);
void initShuffle (JobContext *jc);
void initReduce (JobContext *jc);
void shuffle (JobContext *jc);
IntermediatePair getMaxElement (JobContext *jc);

/* functions for locking/unlocking mutex with error handling */
void lockMutex (pthread_mutex_t *mtx);
void unlockMutex (pthread_mutex_t *mtx);

/* handler for system failures */
void handleSystemError (std::string errorMsg);

/*****************************************************************************/

void lockMutex (pthread_mutex_t *mtx) {
    if (pthread_mutex_lock (mtx) != 0) {
        handleSystemError ("mutex lock failed.");
    }
}

void unlockMutex (pthread_mutex_t *mtx) {
    if (pthread_mutex_unlock (mtx) != 0) {
        handleSystemError ("mutex unlock failed.");
    }
}

void handleSystemError (std::string errorMsg) {
//    printf ("%s \n", SYSTEM_FAILURE_MESSAGE, errorMsg);
    (void) errorMsg;
    exit (SYSTEM_FAILURE_EXIT);
}

/*****************************************************************************/

void defineIntermediateVector (ThreadContext *tc) {
    auto *intermediateVec = new IntermediateVec ();
    if (intermediateVec == nullptr) {
        handleSystemError ("intermediate vector memory allocation failed.");
    }
//    printf ("Defined intermediateVec at address %p for thread %d\n", intermediateVec, tc->threadID);
    tc->intermediateVec = intermediateVec;
}

void initMap (JobContext *jc) {
    /* currently stage is UNDEFINED, change it to MAP */
    (*(jc->stage)) = MAP_STAGE;
//    printf ("stage updated to: %u\n", (*(jc->stage)));
    /* set total to size of InputVec */
    (*(jc->total)) = jc->inputVec->size();
//    printf ("total updated to %u\n", (*(jc->total)));
    /* nullify number of mapped InputPairs */
    (*(jc->processed)) = 0;
//    printf ("processed updated to %u\n", jc->processed->load());
}

void workerMap (ThreadContext *tc) {
    /* as long as there are still InputPairs to map, do so */
    while (tc->jobContext->processed->load() < *(tc->jobContext->total)) {
        lockMutex (tc->jobContext->mutex);
//        printf ("processed %u out of %u hence continues\n", tc->jobContext->processed->load(), *(tc->jobContext->total));
        K1 *key = tc->jobContext->inputVec->at(tc->jobContext->processed->load()).first;
        V1 *value = tc->jobContext->inputVec->at(tc->jobContext->processed->load()).second;
        unlockMutex (tc->jobContext->mutex);
        tc->jobContext->client->map (key, value, tc);
    }
//    printf ("finished map in thread %d\n", tc->threadID);
}

void emit2 (K2 *key, V2 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->intermediateVec->push_back ({key, value});
    (*(tc->jobContext->processed))++;
}

/*****************************************************************************/

void workerSort (ThreadContext *tc) {
    /* sort owns IntermediateVec by the compare function */
    std::sort (tc->intermediateVec->begin(),
               tc->intermediateVec->end(), compare);
}

bool compare (IntermediatePair p1, IntermediatePair p2) {
    return *p1.first < *p2.first;
}

/*****************************************************************************/

void initShuffle (JobContext *jc) {
    /* currently stage is MAP, change it to SHUFFLE */
    (*(jc->stage)) = SHUFFLE_STAGE;
//    printf ("stage updated to: %u\n", (*(jc->stage)));
    /* update total to number of IntermediatePairs,
     * i.e. sum of IntermediateVectors sizes */
    uint32_t new_total = 0;
    for (int i = 0; i < jc->multiThreadLevel; i++) {
        new_total += jc->contexts[i].intermediateVec->size();
    }
    (*(jc->total)) = new_total;
//    printf ("total updated to %u\n", (*(jc->total)));
    /* nullify number of shuffled IntermediatePairs */
    (*(jc->processed)) = 0;
//    printf ("processed updated to %u\n", jc->processed->load());
}

void shuffle (JobContext *jc) {
//    printf("in shuffle function\n");
    auto *shuffledOutput = new std::vector<IntermediateVec> ();
    if (shuffledOutput == nullptr) {
        handleSystemError ("memory allocation for shuffledVector failed.");
    }
    while (true) {
        /* get current max element */
        IntermediatePair maxElement = getMaxElement (jc);
        /* if there is no max element, we are finished */
        if (!maxElement.first) { break; }
        /* create vector of elements that are the same as max element */
        auto *newIntermediateVector = new IntermediateVec( );
        if (newIntermediateVector == nullptr) {
            handleSystemError ("memory allocation for new IntermediateVector failed.");
        }
        for (int i = 0; i < jc->multiThreadLevel; i++) {
            /*  while this vector has more elements and the
             * last element in this vector has max value */
            while (!jc->contexts[i].intermediateVec->empty() &&
                   !compare (jc->contexts[i].intermediateVec->back(), maxElement) &&
                   !compare (maxElement, jc->contexts[i].intermediateVec->back())) {
                IntermediatePair currElement = jc->contexts[i].intermediateVec->back();
                /* add to the key's intermediate vector */
                newIntermediateVector->push_back (currElement);
                /* take off the back of the current vector */
                jc->contexts[i].intermediateVec->pop_back();
            }
        }
//        ::printf("max element vector is: ");
        shuffledOutput->push_back (*newIntermediateVector);
//        printf ("vector size is %zu\n", newIntermediateVector->size());
    }
//    printf ("finished shuffling\n");
//    printf ("shuffled vector length is %zu\n", shuffledOutput->size());
    jc->shuffledOutput = shuffledOutput;
}

IntermediatePair getMaxElement (JobContext *jc) {
    IntermediatePair pair = IntermediatePair ({nullptr, nullptr});
    /* iterate over threads */
    for (int i = 0; i < jc->multiThreadLevel; i++) {
        if (!jc->contexts[i].intermediateVec->empty()) {
            IntermediatePair currElement = jc->contexts[i].intermediateVec->back();
            if (!pair.first || compare (pair, currElement)) {
                pair = currElement;
            }
        }
    }
    return pair;
}

/*****************************************************************************/

void initReduce (JobContext *jc) {
    /* currently stage is SHUFFLE, change it to REDUCE */
    (*(jc->stage)) = REDUCE_STAGE;
//    printf ("stage updated to: %u\n", (*(jc->stage)));
    /* total number of IntermediatePairs to reduce remains the same,
     * hence only need to nullify number of IntermediatePairs to reduce */
    (*(jc->processed)) = 0;
//    printf ("processed updated to %u\n", jc->processed->load());
}

// TODO this is where we stopped yesterday
void workerReduce (ThreadContext *tc) {
    auto intermediateVecNum = tc->jobContext->shuffledOutput->size();
//        printf("reducing in thread\n");
    while (tc->jobContext->processed->load() < *(tc->jobContext->total)) {
        lockMutex (tc->jobContext->mutex);

        /* pop an IntermediateVec from back of shuffledOutput */
        if (tc->jobContext->shuffledOutput->empty()) {
            printf ("error: shuffledOutput should not be empty!!!!!!!!!!!\n");
        }
        IntermediateVec intermediateVec = tc->jobContext->shuffledOutput->back();
        tc->jobContext->shuffledOutput->pop_back();
        /* reduce chosen IntermediateVec */
        tc->jobContext->client->reduce(&intermediateVec, tc);

        unlockMutex (tc->jobContext->mutex);
    }
//    printf("finished reduce\n");
}

void emit3 (K3 *key, V3 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    tc->jobContext->outputVec->push_back ({key, value});
    (*(tc->jobContext->processed))++;
}

/*****************************************************************************/

void *worker (void *arg) {
    /* define arguments for worker */
    auto *tc = (ThreadContext *) arg;

    /* MAP PHASE */

    /* main worker updates job state */
    if (tc->threadID == 0) { initMap (tc->jobContext); }
    /* other workers wait for main worker to finish */
    tc->jobContext->barrier->barrier();

    /* define unique IntermediateVec for map phase */
    defineIntermediateVector (tc);
    /* wait for all workers to define their IntermediateVecs */
    tc->jobContext->barrier->barrier();

    /* carry out MAP phase */
    workerMap (tc);

    /* SORT PHASE */

    /* carry out SORT phase */
    workerSort (tc);
    /* wait for other workers to finish sorting */
    tc->jobContext->barrier->barrier();

    /* SHUFFLE PHASE */

    /* main worker updates job state and carries out SHUFFLE phase */
    if (tc->threadID == 0) {
        initShuffle (tc->jobContext);
        shuffle (tc->jobContext);
    }
    /* threads wait for main thread to finish updating atomicCounter */
    tc->jobContext->barrier->barrier();

    /* REDUCE PHASE */

    /* main worker updates job state */
    if (tc->threadID == 0) { initReduce (tc->jobContext); }
    /* other workers wait for main worker to finish */
    tc->jobContext->barrier->barrier();

    /* carry out REDUCE phase */
    workerReduce (tc);

    return nullptr;
}

/*****************************************************************************/

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    // TODO check memory allocation
    auto *jobContext = new JobContext ();

    auto *threads = new pthread_t[multiThreadLevel];
    auto *contexts = new ThreadContext[multiThreadLevel];

    auto *mutex = new pthread_mutex_t (PTHREAD_MUTEX_INITIALIZER);
    auto *barrier = new Barrier (multiThreadLevel);

    auto *stage = new stage_t (UNDEFINED_STAGE);
    auto *total = new uint32_t (0);
    auto *processed = new std::atomic<uint32_t> (0);

    if (!jobContext || !threads || !contexts || !mutex || !barrier ||
        !stage || !total || !processed) {
        handleSystemError ("failed to allocate memory for jobContext "
                           "or one of its components.");
    }
    *jobContext = (JobContext) {&client, &inputVec, &outputVec, multiThreadLevel,
                                threads, contexts, mutex, barrier,
                                stage, total, processed, nullptr};

    for (int i = 0; i < multiThreadLevel; i++) {
        auto *threadContext = new ThreadContext ();
        if (!threadContext) {
            handleSystemError ("failed to allocate memory for ThreadContext.");
        }
        *threadContext = (ThreadContext) {i, nullptr, jobContext};
        contexts[i] = *threadContext;
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        int res = pthread_create (threads + i, nullptr, worker, contexts + i);
        if (res != 0) {
            handleSystemError ("pthread_create function failed.");
        }
    }

    return static_cast<JobHandle> (jobContext);
}

// TODO this is wrong?
// TODO all threads reach here(?) so first one to get needs to prevent all others
// TODO could be done with shared bool
void waitForJob(JobHandle job) {
    auto *jc = (JobContext *) job;
    for (int i = 0; i < jc->multiThreadLevel; i++){
        if (pthread_join(jc->threads[i], nullptr)){
            std::cout << SYSTEM_FAILURE_MESSAGE << "pthread_join failed"<<std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

void getJobState (JobHandle jc, JobState *state) {
    auto *ctx = (JobContext *) jc;
    lockMutex (ctx->mutex);
    auto stage = *(ctx->stage);
    auto total = *(ctx->total);
    auto processed = ctx->processed->load();
    float processed_percent = (float(processed) / float(total)) * 100;
    *state = {stage_t(stage), processed_percent};
    unlockMutex (ctx->mutex);
}

void closeJobHandle(JobHandle job) {
    //TODO delete all memory
    (void) job;
}