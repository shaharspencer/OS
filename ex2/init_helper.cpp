//
// Created by shaharspencer on 4/19/23.
//

#include "uthreads.cpp"

/**
 * creates main thread
 * called by uthreads.cpp
 * @return SUCCESS upon success else FAILURE
 */
int create_main_thread(std::unique_ptr<Scheduler> scheduler){
    std::shared_ptr<Thread> main_thread = std::make_shared<Thread>(0, main); // TODO how to init thread 0 to main?;
    if (scheduler->add_thread(main_thread)) {
        // TODO handle error
        return SUCCESS;
    }
    return FAILURE;
}
