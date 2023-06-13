#include "VirtualMemory.h"
#include "PhysicalMemory.h"

//TODO arrange function declarations



//
//A frame containing an empty table – where all rows are 0. We don’t have to evict it, but we do have
//        to remove the reference to this table from its parent.
bool isEmptyTable (uint64_t frameIndex) {
    word_t value = 0;
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread (frameIndex * PAGE_SIZE + offset, &value);
        if (value) { return value; }
    }
    return 0;
}

bool isUnusedTable(uint64_t frameIndex){
    word_t value = 0;
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread (frameIndex * PAGE_SIZE + offset, &value);
        if (value) { return value; }
    }
    return 0;
}

typedef bool (*conditionFunc)(uint64_t);


void findUnusedTableHelper (int level, word_t frameIndex, word_t *result, int* maxIndexVisited) {
    if (level == TABLES_DEPTH || ) {
        return;
    }
    bool flag = false;
    word_t value = 0;
    // check if we have children
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread (frameIndex * PAGE_SIZE + offset, &value);
        // if we have found a child, explore that child
        if (value) {
            // update that we have indeed found some child
            flag = true;
            // explore child
            findUnusedTableHelper (++level, value, result, maxIndexVisited);
            // if child returned some empty child of its own return
            if (*result) { return; }
        }
    }

    // if we found no children
    if (!flag){
        *result = frameIndex;
        return;
    }

}


void findEmptyTableHelper (int level, word_t frameIndex, word_t *result) {
    if (level == TABLES_DEPTH) {
        return;
    }
    bool flag = false;
    word_t value = 0;
    // check if we have children
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread (frameIndex * PAGE_SIZE + offset, &value);
        // if we have found a child, explore that child
        if (value) {
            // update that we have indeed found some child
            flag = true;
            // explore child
            findEmptyTableHelper (++level, value, result);
            // if child returned some empty child of its own return
            if (*result) { return; }
        }
    }

    // if we found no children
    if (!flag){
        *result = frameIndex;
        return;
    }

}
/*
 * do DFS to find empty table
 * if we find return the address
 * else return 0
 */
word_t findEmptyTable() {
    // start at root
    word_t emptyTable = 0;
    findEmptyTableHelper(0, 0, &emptyTable);
    return emptyTable;
}



// 2. An unused frame – When traversing the tree, keep a variable with the maximal frame Index ref-
//erenced from any table we visit. Since we fill frames in order, all used frames are contiguous in
//        the memory, and if max_frame_index+1 < NUM_FRAMES then we know that the frame in the index
//(max_frame_index + 1) is unused.




//3. If all frames are already used, then a page must be swapped out from some frame in order to re-
//place it with the relevant page (a table or actual page). The frame that will be chosen is the frame
//        containing a page p such that the cyclical distance: min{NUM_PAGES - |page_swapped_in - p|,
//|page_swapped_in - p|} is maximal (don’t worry about tie-breaking). This page must be swapped
//        out before we use the frame, and we also have to remove the reference to this page from its parent
//table.

void VMinitialize(){
    for (int i = 0; i < PAGE_SIZE; i++){
        PMwrite(i, 0);
    }
}

uint64_t getMask (int level) {
    uint64_t mask = (1LL << OFFSET_WIDTH) - 1;
    for (int i = 0; i < (TABLES_DEPTH - level); i++) { mask << OFFSET_WIDTH; }
    return mask;
}

uint64_t virtualToPhysical (uint64_t virtualAddress) {
    word_t frameIndex = 0;
    for (int level = 0; level < TABLES_DEPTH; level++) {
        uint64_t offset = virtualAddress & getMask (level);
        PMread (frameIndex * PAGE_SIZE + offset, &frameIndex);
        if (frameIndex == 0) {
            // TODO do stuff
            // Find an unused frame or evict a page from some frame. Suppose this frame number is f1
            //Write 0 in all of its contents (only necessary if next layer is a table)
            //PMwrite(0 + 5, f1)
            //addr1 = f1
        }
    }
    if (frameIndex == 0) {
        //Find an unused frame or evict a page from some frame. Suppose this frame number is f2.

        //Make sure you are not evicting f1 by accident
        //Restore the page we are looking for to frame f2 (only necessary pointing to a page)
        //PMwrite(addr1 * PAGE_SIZE + 1, f2)
        //addr2 = f2
    }
    return frameIndex;
}

/* Reads a word from the given virtual address
 * and puts its content in *value.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMread(uint64_t virtualAddress, word_t* value) {
    // if we got an illegal address
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE){
        return 0;
    }
    // convert virtual to physical address
    uint64_t physicalAddress = virtualToPhysical (virtualAddress);
    // TODO check errors
    // finally read the content of the address into value
    PMread (physicalAddress, value);
    return 1;
}

/* Writes a word to the given virtual address.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMwrite(uint64_t virtualAddress, word_t value) {
    uint64_t physicalAddress = virtualToPhysical (virtualAddress);
    PMwrite(physicalAddress, value);
    return 1;
}