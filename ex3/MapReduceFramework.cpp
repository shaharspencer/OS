#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <
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
typedef struct ThreadContext{
    // input vector
    InputVec inputVec;
    //atomic int that represents which elemnt of the input vector we should proccess next
    std::atomic<int>* curr_input;
    MapReduceClient& client;
    OutputVec outputVec;
};

void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){


}

/**
 *
 */
void single_thread_run(void* tc){

    ThreadContext* threadContext = (ThreadContext*) tc;

    /* MAP PHASE */

    // define intermidiary vector for map phase
    auto* intermediateVec = new IntermediateVec(); // TODO memory
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

    /* SHUFFLE PHASE */
}




void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){

}
void closeJobHandle(JobHandle job){

}
