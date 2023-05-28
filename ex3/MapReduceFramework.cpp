#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#include <pthread.h> // for pthread_t, pthread_mutex_t
#include <atomic> // for atomic
#include <string>
#include <set>
#include <algorithm>

using namespace std;

// TODO cleanup code

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
    bool isJobWaiting;
    bool *isWorkerWaiting;
    /* components for keeping track of job progress */
    stage_t stage;
    unsigned long total;
    std::atomic<unsigned long> *processed;
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
void handleSystemError (const char *errorMsg);

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

void handleSystemError (const char *errorMsg) {
    fprintf (stderr, "%s %s\n", SYSTEM_FAILURE_MESSAGE, errorMsg); // TODO stderr only for Mattan tests
    exit (SYSTEM_FAILURE_EXIT);
}

/*****************************************************************************/

void defineIntermediateVector (ThreadContext *tc) {
    printf("allocating intermidiate vector for thread %d\n", tc->threadID);
    try{
        auto *intermediateVec = new IntermediateVec ();
        tc->intermediateVec = intermediateVec;
    }
    catch (const std::bad_alloc& b)
    {
        handleSystemError ("intermediate vector memory allocation failed.");
    }


}

void initMap (JobContext *jc) {
    lockMutex (jc->mutex);
    /* currently stage is UNDEFINED, change it to MAP */
    jc->stage = MAP_STAGE;
    /* set total to number of InputPairs,
     * i.e. sum of InputVectors sizes */
    jc->total = jc->inputVec->size();
    /* nullify number of mapped InputPairs */
    (*(jc->processed)) = 0;
    unlockMutex (jc->mutex);
}

void workerMap (ThreadContext *tc) {
    lockMutex (tc->jobContext->mutex);
    auto total = tc->jobContext->total;
    unlockMutex (tc->jobContext->mutex);
    /* as long as there are still InputPairs to map, do so */
    while (tc->jobContext->processed->load() < total) {
        lockMutex (tc->jobContext->mutex);
        K1 *key = tc->jobContext->inputVec->at(tc->jobContext->processed->load()).first;
        V1 *value = tc->jobContext->inputVec->at(tc->jobContext->processed->load()).second;
        (*(tc->jobContext->processed))++;
        unlockMutex (tc->jobContext->mutex);
        tc->jobContext->client->map (key, value, tc);
    }
}

void emit2 (K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;
//    lockMutex (tc->jobContext->mutex);
    tc->intermediateVec->push_back ({key, value});
//    unlockMutex (tc->jobContext->mutex);
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
    lockMutex (jc->mutex);
    /* currently stage is MAP, change it to SHUFFLE */
    jc->stage = SHUFFLE_STAGE;
    unsigned long newTotal = 0;
    for (int i = 0; i < jc->multiThreadLevel; i++) {
        newTotal += jc->contexts[i].intermediateVec->size();
    }
    jc->total = newTotal;
    /* total number of IntermediatePairs to shuffle is the same,
     * hence only need to nullify number of IntermediatePairs to shuffle */
    (*(jc->processed)) = 0;
    unlockMutex (jc->mutex);
}

void shuffle (JobContext *jc) {
    lockMutex (jc->mutex);
    std::vector<IntermediateVec> *shuffledOutput;
    printf("defining shuffledoutput vector");
    try{
        shuffledOutput = new std::vector<IntermediateVec> ();
    }
    catch (std::bad_alloc& b){
        handleSystemError ("memory allocation for shuffledVector failed.");
    }

    while (true) {
        /* get current max element */
        IntermediatePair maxElement = getMaxElement (jc);
        /* if there is no max element, we are finished */
        if (!maxElement.first) { break; }
        /* create vector of elements that are the same as max element */
        IntermediateVec * newIntermediateVector;
        try{
            newIntermediateVector = new IntermediateVec( );
        }
        catch (std::bad_alloc& b){
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
        shuffledOutput->push_back (*newIntermediateVector);
    }
    jc->shuffledOutput = shuffledOutput;
    unlockMutex (jc->mutex);
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
    lockMutex (jc->mutex);
    /* currently stage is SHUFFLE, change it to REDUCE */
    jc->stage = REDUCE_STAGE;
    jc->total = jc->shuffledOutput->size();
    /* total number of IntermediatePairs to reduce remains the same,
     * hence only need to nullify number of IntermediatePairs to reduce */
    (*(jc->processed)) = 0;
    unlockMutex (jc->mutex);
}

void workerReduce (ThreadContext *tc) {
    lockMutex (tc->jobContext->mutex);
    auto total = tc->jobContext->total;
    unlockMutex (tc->jobContext->mutex);
    while ((tc->jobContext->processed->load()) < total) {
        lockMutex (tc->jobContext->mutex);
        /* pop an IntermediateVec from back of shuffledOutput */
        if (tc->jobContext->shuffledOutput->empty()) {
            printf ("error: shuffledOutput should not be empty!!!!!!!!!!!\n");
        }
        IntermediateVec intermediateVec = tc->jobContext->shuffledOutput->back();
        tc->jobContext->shuffledOutput->pop_back();
        /* reduce chosen IntermediateVec */
        tc->jobContext->client->reduce(&intermediateVec, tc);
        (*(tc->jobContext->processed))++;
        unlockMutex (tc->jobContext->mutex);
    }
}

void emit3 (K3 *key, V3 *value, void *context) {
    auto *tc = (ThreadContext *) context;
//    lockMutex (tc->jobContext->mutex);
    tc->jobContext->outputVec->push_back ({key, value});
//    unlockMutex (tc->jobContext->mutex);
}

/*****************************************************************************/

void *worker (void *arg) {
    /* define arguments for worker */
    auto *tc = (ThreadContext *) arg;

    /* MAP PHASE */

    /* main worker updates job state */
    if (tc->threadID == 0) { initMap (tc->jobContext); }
    /* other workers isJobWaiting for main worker to finish */
    tc->jobContext->barrier->barrier();

    /* define unique IntermediateVec for map phase */
    defineIntermediateVector (tc);
    /* isJobWaiting for all workers to define their IntermediateVecs */
    tc->jobContext->barrier->barrier();

    /* carry out MAP phase */
    workerMap (tc);

    /* SORT PHASE */

    /* carry out SORT phase */
    workerSort (tc);
    /* isJobWaiting for other workers to finish sorting */
    tc->jobContext->barrier->barrier();

    /* SHUFFLE PHASE */

    /* main worker updates job state and carries out SHUFFLE phase */
    if (tc->threadID == 0) {
        initShuffle (tc->jobContext);
        shuffle (tc->jobContext);
    }
    /* threads isJobWaiting for main thread to finish updating atomicCounter */
    tc->jobContext->barrier->barrier();

    /* REDUCE PHASE */

    /* main worker updates job state */
    if (tc->threadID == 0) { initReduce (tc->jobContext); }
    /* other workers isJobWaiting for main worker to finish */
    tc->jobContext->barrier->barrier();

    /* carry out REDUCE phase */
    workerReduce (tc);

    return nullptr;
}

/*****************************************************************************/

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    JobContext* jobContext;
    pthread_t* threads;
    ThreadContext * contexts;
    pthread_mutex_t * mutex;
    Barrier* barrier;
    bool* isWorkerWaiting;
    std::atomic<unsigned long>* processed;

    try{
        jobContext = new JobContext ();
        threads = new pthread_t[multiThreadLevel];
        contexts = new ThreadContext[multiThreadLevel];
        mutex = new  pthread_mutex_t (PTHREAD_MUTEX_INITIALIZER);
        barrier = new Barrier (multiThreadLevel);
        isWorkerWaiting = new bool[multiThreadLevel];
        processed = new std::atomic<unsigned long> (0);
    }
    catch (const std::bad_alloc& b){
        handleSystemError ("failed to allocate memory for jobContext "
                           "or one of its components.");
    }


    *jobContext = (JobContext) {&client, &inputVec, &outputVec, multiThreadLevel,
                                threads, contexts, mutex, barrier,
                                false, isWorkerWaiting,
                                UNDEFINED_STAGE, 0, processed, nullptr};

    for (int i = 0; i < multiThreadLevel; i++) {
        ThreadContext * threadContext;
        try{
            threadContext = new ThreadContext ();
        }
        catch (const std::bad_alloc& b){
            handleSystemError ("failed to allocate memory for ThreadContext.");
        }


        *threadContext = (ThreadContext) {i, nullptr, jobContext};
        contexts[i] = *threadContext;
        isWorkerWaiting[i] = false;
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        int res = pthread_create (threads + i, nullptr, worker, contexts + i);
        if (res != 0) {
            handleSystemError ("pthread_create function failed.");
        }
    }

    printf ("Created job!\n");
    return static_cast<JobHandle> (jobContext);
}

void waitForJob (JobHandle job) {
    auto *jc = (JobContext *) job;
    if (!jc->isJobWaiting) {
        jc->isJobWaiting = true;
        for (int i = 0; i < jc->multiThreadLevel; i++) {
            if (jc->isWorkerWaiting[i]) {
                return;
            }
            if (pthread_join (jc->threads[i], nullptr) != 0) {
                handleSystemError ("pthread_join failed.");
            }
            jc->isWorkerWaiting[i] = true;
        }
    }
}

void getJobState (JobHandle jc, JobState *state) {
    auto *ctx = (JobContext *) jc;
    lockMutex (ctx->mutex);
    auto stage = ctx->stage;
    auto total = ctx->total;
    auto processed = ctx->processed->load();
    float processed_percent = (float(processed) / float(total)) * 100;
    *state = {stage, processed_percent};
    unlockMutex (ctx->mutex);
}

void closeJobHandle (JobHandle job) {
    // TODO check no memory leaks occur
    waitForJob (job);
    auto *jc = (JobContext *) job;
    delete[] jc->threads;
    for (int i = 0; i < jc->multiThreadLevel; i++) {
        delete jc->contexts[i].intermediateVec;
    }
    delete[] jc->contexts;
    pthread_mutex_destroy (jc->mutex);
    delete jc->barrier;
    delete[] jc->isWorkerWaiting;
    delete jc->processed;
    delete jc->shuffledOutput;
    delete jc;
}