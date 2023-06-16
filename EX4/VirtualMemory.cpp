#include "VirtualMemory.h"
#include "PhysicalMemory.h"

//TODO arrange function declarations!!

void trimFrame(word_t parentFrameIndex, word_t childFrameIndex) {
    word_t value;
    /* iteratively look for the child frame index in the parent frame */
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread(parentFrameIndex * PAGE_SIZE + offset, &value);
        /* once found, remove it by updating value to 0 and return */
        if (value == childFrameIndex) {
            PMwrite(parentFrameIndex * PAGE_SIZE + offset, 0);
            return;
        }
    }
}

//
//A frame containing an empty table – where all rows are 0. We don’t have to evict it, but we do have
//        to remove the reference to this table from its parent.
// TODO do we use this?
bool isEmptyTable(uint64_t frameIndex) {
    word_t value = 0;
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread(frameIndex * PAGE_SIZE + offset, &value);
        if (value) { return value; }
    }
    return 0;
}

//bool isUnusedTable(uint64_t frameIndex) {
//    word_t value = 0;
//    for (int offset = 0; offset < PAGE_SIZE; offset++) {
//        PMread(frameIndex * PAGE_SIZE + offset, &value);
//        if (value) { return value; }
//    }
//    return 0;
//}

//void findUnusedTableHelper(int level, word_t frameIndex, word_t *result, int *maxIndexVisited) {
//    if (level == TABLES_DEPTH ||) {
//        return;
//    }
//    bool flag = false;
//    word_t value = 0;
//    // check if we have children
//    for (int offset = 0; offset < PAGE_SIZE; offset++) {
//        PMread(frameIndex * PAGE_SIZE + offset, &value);
//        // if we have found a child, explore that child
//        if (value) {
//            // update that we have indeed found some child
//            flag = true;
//            // explore child
//            findUnusedTableHelper(++level, value, result, maxIndexVisited);
//            // if child returned some empty child of its own return
//            if (*result) { return; }
//        }
//    }
//
//    // if we found no children
//    if (!flag) {
//        *result = frameIndex;
//        return;
//    }
//
//}


void findEmptyTableHelper(int level, word_t prevFrameIndex, word_t frameIndex,
                          word_t *parentFrameIndex, word_t *result) {
    /* if we've reached a leaf, values refer to VM hence return */
    if (level == TABLES_DEPTH) { return; }
    /* assume current frame is empty i.e. has no children.
     * if we figure otherwise this flag will change to true
     * and nextFrameIndex will update to child */
    bool flag = false;
    word_t nextFrameIndex = 0;
    /* iteratively look for children */
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread(frameIndex * PAGE_SIZE + offset, &nextFrameIndex);
        /* if we have found a child, call recursively with that child as frameIndex */
        if (nextFrameIndex) {
            /* update that we have indeed found a child */
            flag = true;
            /* explore that child */
            findEmptyTableHelper(++level, prevFrameIndex, nextFrameIndex,
                                 &frameIndex, result);
            /* if child has returned some empty child of its own,
             * prevent traversing other children by returning */
            if (*result) { return; }
        }
    }
    /* if we found no children and only if frame isn't last frame mutated */
    if (!flag && frameIndex != prevFrameIndex) {
        /* trim frame from its parent */
        trimFrame(*parentFrameIndex, frameIndex)
        * result = frameIndex;
    }
}

/*
 * do DFS to find empty table
 * if we find return the address
 * else return 0
 */
word_t findEmptyTable(word_t prevFrameIndex) {
    /* default value for if we do not find an empty table */
    word_t emptyTable = -1;
    /* recursively look for an empty table, starting from the root table 0 */
    word_t parentFrameIndex = 0;
    findEmptyTableHelper(0, prevFrameIndex, 0,
                         &parentFrameIndex, &emptyTable);
    return emptyTable;
}

void findUnusedFrameHelper(int level, word_t frameIndex, uint64_t *maxUsedFrameIndex) {
    /* if we've reached a leaf, values refer to VM hence return */
    if (level == TABLES_DEPTH) { return; }
    /* iteratively and recursively look for max used frame index */
    word_t nextFrameIndex = 0;
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMread(frameIndex * PAGE_SIZE + offset, &nextFrameIndex);
        /* if frame has child, call on it recursively */
        if (nextFrameIndex) {
            /* if child's index is higher than current max used frame index, update it */
            if (nextFrameIndex > *maxUsedFrameIndex) { *maxUsedFrameIndex = nextFrameIndex; }
            /* continue recursion */
            findUnusedFrameHelper(++level, nextFrameIndex, maxUsedFrameIndex);
        }
    }
}

word_t findUnusedFrame(word_t frameIndex) {
    word_t maxUsedFrameIndex = frameIndex;
    /* recursively look for max unused frame index, starting from the root table 0 */
    findUnusedFrameHelper(0, 0, &maxUsedFrameIndex);
    return maxUsedFrameIndex;
}

word_t findFrameToEvict(/* TODO args */) {}

void VMinitialize() {
    for (int i = 0; i < PAGE_SIZE; i++) {
        PMwrite(i, 0);
    }
}

uint64_t getOffsetMask(int level) {
    uint64_t mask = (1LL << OFFSET_WIDTH) - 1;
    for (int i = 0; i < (TABLES_DEPTH - level); i++) { mask << OFFSET_WIDTH; }
    return mask;
}

void nullifyFrame(uint64_t frameIndex) {
    for (int offset = 0; offset < PAGE_SIZE; offset++) {
        PMwrite(frameIndex * PAGE_SIZE + offset, 0);
    }
}

void prepareForTreeMutation(word_t frameIndex, word_t *nextFrameIndex) {
    nullifyFrame(frameIndex);
    *nextFrameIndex = 0;
}

void mutateTree(word_t prevFrameIndex, word_t frameIndex, int offset) {
    /* first look for an empty table. if found, update accordingly */
    word_t emptyTable = findEmptyTable(prevFrameIndex);
    if (emptyTable != -1) {
        PMwrite(prevFrameIndex * PAGE_SIZE + offset, emptyTable);
        return;
    }
    /* next, look for an unused frame. if found, update accordingly */
    word_t unusedFrameIndex = findUnusedFrame(frameIndex) + 1;
    if (unusedFrameIndex < NUM_FRAMES) {
        PMwrite(prevFrameIndex * PAGE_SIZE + offset, unusedFrameIndex);
        return;
    }
    /* finally, evict a frame by max cyclic distance policy */
    word_t frameToEvict = findFrameToEvict(); // TODO args
    // TODO evict frame, update tree (in function), if not leaf than nullify
}

uint64_t virtualToPhysical(uint64_t virtualAddress) {
    word_t frameIndex = 0, nextFrameIndex = 0, prevFrameIndex = 0;
    uint64_t offset = 0;
    for (int level = 0; level < TABLES_DEPTH; level++) {
        /* get relevant bits from virtual address */
        offset = virtualAddress & getOffsetMask(level);
        /* shift bits to the right to get actual offset */
        offset = offset >> (VIRTUAL_ADDRESS_WIDTH - OFFSET_WIDTH * (level + 2));
        /* get next frame index */
        PMread(frameIndex * PAGE_SIZE + offset, &nextFrameIndex);
        /* if next frame index exceeds available RAM, prepare for tree mutation */
        if (nextFrameIndex >= NUM_FRAMES) {
            prepareForTreeMutation(frameIndex, &nextFrameIndex);
        }
        /* if tree mutation is needed, execute it, otherwise continue */
        if (nextFrameIndex == 0) {
            mutateTree(); // TODO what should be sent
        } else {
            frameIndex = nextFrameIndex;
        }
        prevFrameIndex = frameIndex;
    }
    /* restore called page to RAM */
    uint64_t restoredPageIndex = virtualAddress & ~OFFSET_WIDTH;
    restoredPageIndex = restoredPageIndex >> OFFSET_WIDTH;
    PMrestore(prevFrameIndex, restoredPageIndex)
    return (frameIndex * PAGE_SIZE + offset);
}

/* Reads a word from the given virtual address
 * and puts its content in *value.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMread(uint64_t virtualAddress, word_t *value) {
    // if we got an illegal address
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE) { return 0; }
    // convert virtual to physical address
    uint64_t physicalAddress = virtualToPhysical(virtualAddress);
    // finally read the content of the address into value
    PMread(physicalAddress, value);
    return 1;
}

/* Writes a word to the given virtual address.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMwrite(uint64_t virtualAddress, word_t value) {
    /* same as VMread, except calls PMwrite instead of PMread */
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE) { return 0; }
    uint64_t physicalAddress = virtualToPhysical(virtualAddress);
    PMwrite(physicalAddress, value);
    return 1;
}