#include "VirtualMemory.h"
#include "PhysicalMemory.h"

/* this is the star of the show, get physical address
 * and manipulate RAM when needed */
uint64_t virtualToPhysical(uint64_t virtualAddress);
uint64_t getOffsetMask(int level);

/* whenever a page fault occurs, these functions are used */
void prepareForTreeMutation(word_t frameIndex, word_t *nextFrameIndex);
void mutateTree(word_t prevFrameIndex, word_t *frameIndex, int offset,
                uint64_t pageToRestore, int level);
void trimFrame(word_t parentFrameIndex, word_t childFrameIndex);

/* used by VMinitialize and on eviction when necessary */
void nullifyFrame(uint64_t frameIndex);

/* recursive functions for executing page fault policy */
word_t findEmptyTable(word_t prevFrameIndex);
void findEmptyTableHelper(int level, word_t prevFrameIndex, word_t frameIndex,
                          const word_t *parentFrameIndex, word_t *result);
word_t findUnusedFrame(word_t frameIndex);
void findUnusedFrameHelper(int level, word_t frameIndex, word_t *maxUsedFrameIndex);
word_t findFrameToEvict(uint64_t pageToRestore);
void findFrameToEvictHelper(int level, word_t prevFrameIndex, word_t frameIndex,
                            word_t *frameToEvict, word_t *parentFrameIndex,
                            int pageNumber, int pageToRestore, int *pageToEvict, int bits);
int cyclicDistance(int pageIndex, int otherPageIndex);
int abs(int num);

/*************************************************************/

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

void findEmptyTableHelper(int level, word_t prevFrameIndex, word_t frameIndex,
                          const word_t *parentFrameIndex, word_t *result) {
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
            findEmptyTableHelper(level + 1, prevFrameIndex, nextFrameIndex,
                                 &frameIndex, result);
            /* if child has returned some empty child of its own,
             * prevent traversing other children by returning */
            if (*result > 0) { return; }
        }
    }
    /* if we found no children and only if frame isn't last frame mutated */
    if (!flag && frameIndex != prevFrameIndex) {
        /* trim frame from its parent */
        trimFrame(*parentFrameIndex, frameIndex);
        *result = frameIndex;
    }
}

word_t findEmptyTable(word_t prevFrameIndex) {
    /* default value for if we do not find an empty table */
    word_t emptyTable = -1;
    /* recursively look for an empty table, starting from the root table 0 */
    word_t parentFrameIndex = 0;
    findEmptyTableHelper(0, prevFrameIndex, 0,
                         &parentFrameIndex, &emptyTable);
    return emptyTable;
}

void findUnusedFrameHelper(int level, word_t frameIndex, word_t *maxUsedFrameIndex) {
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
            findUnusedFrameHelper(level + 1, nextFrameIndex, maxUsedFrameIndex);
        }
    }
}

word_t findUnusedFrame(const word_t frameIndex) {
    word_t maxUsedFrameIndex = frameIndex;
    /* recursively look for max unused frame index, starting from the root table 0 */
    findUnusedFrameHelper(0, 0, &maxUsedFrameIndex);
    return maxUsedFrameIndex;
}

int abs(const int num) {
    int res = (num >= 0) ? num : (-num);
    return res;
}

int cyclicDistance(const int pageIndex, const int otherPageIndex) {
    int dist1 = abs(pageIndex - otherPageIndex);
    int dist2 = NUM_PAGES - dist1;
    int res = (dist1 <= dist2) ? dist1 : dist2;
    return res;
}

void findFrameToEvictHelper(int level, word_t prevFrameIndex, word_t frameIndex,
                            word_t *frameToEvict, word_t *parentFrameIndex,
                            int pageNumber, const int pageToRestore, int *pageToEvict, int bits) {
    /* upon reaching a leaf, compare its cyclic distance to current farthest.
     * if distance is larger, update accordingly */
    if (level == TABLES_DEPTH) {
        /* if no page was selected yet, or if current page's cyclic distance
         * is larger than previous, select current page */
        if (*frameToEvict == 0 || (cyclicDistance(pageNumber, pageToRestore) >
                                   cyclicDistance(*pageToEvict, pageToRestore))) {
            *frameToEvict = frameIndex;
            *pageToEvict = pageNumber;
            *parentFrameIndex = prevFrameIndex;
        }
        return;
    }
    /* iteratively and recursively traverse the tree while constructing the page number */
    word_t nextFrameIndex = 0;
    for (uint64_t offset = 0; offset < PAGE_SIZE; offset++) {
        PMread(frameIndex * PAGE_SIZE + offset, &nextFrameIndex);
        if (nextFrameIndex) {
            /* construct page number by offset */
            int updatedPageNumber = pageNumber << ((bits - OFFSET_WIDTH >= OFFSET_WIDTH)
                                                   ? OFFSET_WIDTH : (bits - OFFSET_WIDTH));
            updatedPageNumber += offset;
            /* continue recursion */
            findFrameToEvictHelper(level + 1, frameIndex, nextFrameIndex, frameToEvict,
                                   parentFrameIndex, updatedPageNumber, pageToRestore, pageToEvict,
                                   bits - OFFSET_WIDTH);
        }
    }
}

word_t findFrameToEvict(uint64_t pageToRestore) {
    /* default values for if we do not find a frame to evict */
    word_t frameToEvict = 0;
    int pageToEvict = 0;
    /* recursively look for the frame to evict by max cyclic distance policy,
     * starting from the root table 0 */
    word_t parentFrameIndex = 0;
    findFrameToEvictHelper(0, 0, 0, &frameToEvict, &parentFrameIndex,
                           0, pageToRestore, &pageToEvict, VIRTUAL_ADDRESS_WIDTH);
    /* mutate the tree by results */
    trimFrame(parentFrameIndex, frameToEvict);
    /* evict decided page from RAM */
    PMevict(frameToEvict, pageToEvict);
    return frameToEvict;
}

uint64_t getOffsetMask(int level) {
    uint64_t mask = (1 << OFFSET_WIDTH) - 1;
    for (int i = 0; i < (TABLES_DEPTH - level); i++) { mask <<= OFFSET_WIDTH; }
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

void mutateTree(word_t prevFrameIndex, word_t *frameIndex, int offset,
                uint64_t pageToRestore, int level) {
    /* first look for an empty table. if found, update accordingly */
    word_t emptyTable = findEmptyTable(prevFrameIndex);
    if (emptyTable != -1) {
        PMwrite(prevFrameIndex * PAGE_SIZE + offset, emptyTable);
        *frameIndex = emptyTable;
        return;
    }
    /* next, look for an unused frame. if found, update accordingly */
    word_t unusedFrameIndex = findUnusedFrame(*frameIndex) + 1;
    if (unusedFrameIndex < NUM_FRAMES) {
        PMwrite(prevFrameIndex * PAGE_SIZE + offset, unusedFrameIndex);
        *frameIndex = unusedFrameIndex;
        return;
    }
    /* finally, evict a frame by max cyclic distance policy */
    word_t frameToEvict = findFrameToEvict(pageToRestore);
    /* if evicted frame isn't a leaf, nullify it */
    if (level != TABLES_DEPTH - 1) { nullifyFrame(frameToEvict); }
    PMwrite(prevFrameIndex * PAGE_SIZE + offset, frameToEvict);
    *frameIndex = frameToEvict;
}

uint64_t virtualToPhysical(uint64_t virtualAddress) {
    word_t frameIndex = 0, nextFrameIndex = 0, prevFrameIndex = 0;
    /* calculate the page number which will be restored to RAM */
    uint64_t restoredPageIndex = virtualAddress & ~OFFSET_WIDTH;
    restoredPageIndex = restoredPageIndex >> OFFSET_WIDTH;
    for (int level = 0; level < TABLES_DEPTH; level++) {
        /* get relevant bits from virtual address */
        uint64_t mask = getOffsetMask(level);
        uint64_t offset = virtualAddress & mask;
        /* shift bits to the right to get actual offset */
        for (int i = level; i < TABLES_DEPTH; i++) {
            offset >>= OFFSET_WIDTH;
        }
        /* get next frame index */
        PMread(frameIndex * PAGE_SIZE + offset, &nextFrameIndex);
        /* if next frame index exceeds available RAM, prepare for tree mutation */
        if (nextFrameIndex >= NUM_FRAMES) {
            prepareForTreeMutation(frameIndex, &nextFrameIndex);
        }
        /* if tree mutation is needed, execute it, otherwise continue */
        if (nextFrameIndex == 0) {
            mutateTree(prevFrameIndex, &frameIndex, offset, restoredPageIndex, level);
        } else {
            frameIndex = nextFrameIndex;
        }
        prevFrameIndex = frameIndex;
    }
    /* restore called page to RAM */
    PMrestore(prevFrameIndex, restoredPageIndex);
    uint64_t pageOffset = virtualAddress & getOffsetMask(TABLES_DEPTH);
    return (frameIndex * PAGE_SIZE + pageOffset);
}

/*****************************************************************/

void VMinitialize() {
    for (int i = 0; i < NUM_FRAMES; i++) {
        nullifyFrame(i);
    }
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