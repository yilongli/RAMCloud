/* Copyright (c) 2011-2018 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

// #include <sched.h>

#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 5) || (__GNUC__ > 4)
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <sys/resource.h>
#include <sys/time.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <boost/program_options.hpp>
#include <byteswap.h>

#include "Common.h"
#include "Cycles.h"
#include "simulator/Merge.h"

using namespace RAMCloud;

/*
 * This function just discards its argument. It's used to make it
 * appear that data is used,  so that the compiler won't optimize
 * away the code we're trying to measure.
 *
 * \param value
 *      Pointer to arbitrary value; it's discarded.
 */
void discard(void* value) {
    int x = *reinterpret_cast<int*>(value);
    if (x == 0x43924776) {
        printf("Value was 0x%x\n", x);
    }
}

//----------------------------------------------------------------------
// Test functions start here
//----------------------------------------------------------------------


/**
 * Fill the given address with random data.
 *
 * \param dest
 *      Memory block in which to write in random data.
 * \param length
 *      Total number of bytes to write.
 */
void fillRandom(char* dest, uint32_t length)
{
    memset(dest, 'x', length);
    for (uint offset = 0; offset + 8 <= length; offset += 8) {
        *(reinterpret_cast<uint64_t*>(dest + offset)) = generateRandom();
    }
    int bytesLeftToFill = length & 7;
    for (uint offset = length - bytesLeftToFill; offset < length; offset++) {
        *(dest + offset) = downCast<char>(generateRandom() & 127);
    }
}

double memcmp10b(){
    int count = 100000;
    char key1[1024][10], key2[1024][10];
    for (int i = 0; i < 1000; i++) {
        fillRandom(key1[i], 10);
        fillRandom(key2[i], 10);
    }
    
    int countSmallerThan = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        if (memcmp(key1[i & 1023], key2[i & 1023], 10) > 0)
            countSmallerThan++;
    }
    uint64_t stop = Cycles::rdtscp();
    
    printf("countSmallerThan: %d\n", countSmallerThan);

    return Cycles::toSeconds(stop - start)/(count);
}

double memcpy16b(){
    int count = 1000000;
    const int size = 16;
    char key1[1024][size], key2[1024][size];
    for (int i = 0; i < 1000; i++) {
        fillRandom(key1[i], size);
        fillRandom(key2[i], size);
    }

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        memcpy(key1[i & 1023], key2[i & 1023], size);
    }
    uint64_t stop = Cycles::rdtscp();
    
    return Cycles::toSeconds(stop - start)/(count);
}

struct MillisortArrayPtr {
    MillisortItem* data;
    size_t size;
};

void mergeByMinheap(std::vector<std::vector<MillisortItem>*> &vectors, std::vector<MillisortItem>* outputBuffer)
{
    struct PqItem {
        char* key;
        int vectorId;
        PqItem& operator=(PqItem other)
        {
            key = other.key;
            vectorId = other.vectorId;
            return *this;
        }
    };
    class PqComparator {
    public:
        bool operator() (const PqItem& w1, const PqItem& w2)
        {
            return std::memcmp(w1.key, w2.key, MillisortItem::MillisortKeyLen) < 0;
        }
    };
    std::priority_queue<PqItem, std::vector<PqItem>, PqComparator> pq;
    uint nextIds[vectors.size()] = {0, };
    for (uint i = 0; i < vectors.size(); i++) {
        PqItem item;
        item.key = vectors[i]->at(0).key;
        item.vectorId = i;
        pq.push(item);
    }
    
    while (!pq.empty()) {
        PqItem minItem = pq.top();
        MillisortItem& value = vectors[minItem.vectorId]->at(nextIds[minItem.vectorId]++);
        outputBuffer->push_back(value);
        pq.pop();
        PqItem nextItem;
        if (nextIds[minItem.vectorId] < vectors[minItem.vectorId]->size()) {
            nextItem.key = vectors[minItem.vectorId]->at(nextIds[minItem.vectorId]).key;
            nextItem.vectorId = minItem.vectorId;
            pq.push(nextItem);
        }
    }
}

void mergeBySort(std::vector<std::vector<MillisortItem>*> &vectors, std::vector<MillisortItem>* outputBuffer)
{
    uint64_t start = Cycles::rdtscp();
    for (uint i = 0; i < vectors.size(); i++) {
        outputBuffer->insert(outputBuffer->end(),
                vectors[i]->begin(), vectors[i]->end());
    }
    uint64_t copyDone = Cycles::rdtscp();
    std::sort(outputBuffer->begin(), outputBuffer->end());
    uint64_t sortDone = Cycles::rdtscp();
    
    printf("Copy insert: %.2f ms\n", Cycles::toSeconds(copyDone - start) * 1000);
    printf("std::sort: %.2f ms\n", Cycles::toSeconds(sortDone - copyDone) * 1000);
}

double mergeAllData()
{
    int numMasters = 1024;
    int itemsPerNode = 50;
//    int numMasters = 32;
//    int itemsPerNode = 50 * 32;
//    int numMasters = 8;
//    int itemsPerNode = 50 * 128;
//    int numMasters = 2;
//    int itemsPerNode = 50 * 512;
    
    std::vector<MillisortItem> vectors[numMasters];
    std::vector<std::vector<MillisortItem>*> vectorPtrs;
    for (int nodeId = 0; nodeId < numMasters; nodeId++) {
//        vectors[nodeId].resize(itemsPerNode);
        for (int i = 0; i < itemsPerNode; i++) {
            MillisortItem item;
            fillRandom(item.key, MillisortItem::MillisortKeyLen);
            fillRandom(item.data, MillisortItem::MillisortValueLen);
            vectors[nodeId].push_back(item);
        }
        std::sort(vectors[nodeId].begin(), vectors[nodeId].end());
        vectorPtrs.push_back(&vectors[nodeId]);
    }
    std::vector<MillisortItem> outputBuffer;
    
    uint64_t start = Cycles::rdtscp();
    mergeByMinheap(vectorPtrs, &outputBuffer);
    // mergeBySort(vectorPtrs, &outputBuffer);
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start);
}

double mergeStrategy()
{
    int numMasters = 1024;
    int itemsPerNode = 50;
//    int numMasters = 32;
//    int itemsPerNode = 50 * 32;
//    int numMasters = 8;
//    int itemsPerNode = 50 * 128;
//    int numMasters = 2;
//    int itemsPerNode = 50 * 512;
    
//    int usecToReceiveMsg = 0.7;

    for (int ways = 2; ways <= numMasters / 8; ways *= 2) {
        printf("**** Merge ways: %d ****\n", ways);
        double aggregateTime = 0;
        for (int level = 0; pow(ways, level) < numMasters; level++) {
            int numArrays = ways;
            int itemsPerArray = downCast<int>(itemsPerNode * pow(ways, level));
            if (pow(ways, level+1) > numMasters) {
                numArrays = downCast<int>(numMasters / pow(ways, level));
            }
        
            std::vector<MillisortItem> vectors[numArrays];
            std::vector<std::vector<MillisortItem>*> vectorPtrs;
            for (int nodeId = 0; nodeId < numArrays; nodeId++) {
                //        vectors[nodeId].resize(itemsPerNode);
                for (int i = 0; i < itemsPerArray; i++) {
                    MillisortItem item;
                    fillRandom(item.key, MillisortItem::MillisortKeyLen);
                    fillRandom(item.data, MillisortItem::MillisortValueLen);
                    vectors[nodeId].push_back(item);
                }
                std::sort(vectors[nodeId].begin(), vectors[nodeId].end());
                vectorPtrs.push_back(&vectors[nodeId]);
            }
            std::vector<MillisortItem> outputBuffer;
    
            uint64_t start = Cycles::rdtscp();
            mergeByMinheap(vectorPtrs, &outputBuffer);
            // mergeBySort(vectorPtrs, &outputBuffer);
            uint64_t stop = Cycles::rdtscp();
            double mergeTimeMs = Cycles::toSeconds(stop - start) * 1000;
            double totalTimeMs = mergeTimeMs * (numMasters / pow(ways, level) / numArrays);
            printf("# of arrays: %d, # of items per array: %d ==> %.2f ms, (total with 7 workerCores: %.2f)\n",
                    numArrays, itemsPerArray, Cycles::toSeconds(stop - start) * 1000, totalTimeMs / 7);
            aggregateTime += totalTimeMs;
        }
        printf(" Total aggregate: %.2f ms (%.2f ms on 8 cores -- 7 workerCores)\n", aggregateTime, aggregateTime / 7);
    }
    
    return 0;
}

struct MergeWorkerContext {
    std::atomic<int> state; // 0: idle, 1: done, 2: working. 3: die.
    
    // Description of Job.
    std::vector<MillisortArrayPtr> arrays;
    MillisortArrayPtr dest;
    
    // Performance statistics
    uint64_t workCycles = 0;
    uint64_t idleTotalCycles = 0;
    uint64_t noWorkCycles = 0;
};

void printData(const MillisortItem* ptr, size_t length, const char* header = "", bool warnIfUnsorted = true) {
    printf("%s ", header);
    uint64_t prev = 0;
    for (size_t i = 0; i < length; i++) {
        uint64_t raw = *((const uint64_t*)ptr[i].key);
        uint64_t swapped = bswap_64(raw);
        if (warnIfUnsorted && i > 0 && prev > swapped) {
            printf(" *");
        }
        prev = swapped;
//        printf(" %22" PRIu64 "", swapped);
        printf(" %3" PRIu64 "", swapped);
    }
    printf(" |");
//    fflush(stdout);
}


void printData(MillisortArrayPtr ptr, const char* header = "", bool warnIfUnsorted = true) {
    printf("%s ", header);
    uint64_t prev = 0;
    for (size_t i = 0; i < ptr.size; i++) {
        uint64_t raw = *((uint64_t*)ptr.data[i].key);
        uint64_t swapped = bswap_64(raw);
        if (warnIfUnsorted && i > 0 && prev > swapped) {
            printf(" *");
        }
        prev = swapped;
//        printf(" %22" PRIu64 "", swapped);
        printf(" %3" PRIu64 "", swapped);
    }
    printf(" |");
//    fflush(stdout);
}

#define VERBOSE 0

void mergeWorker(MergeWorkerContext* context) {
//    uint64_t lastIdleStart = Cycles::rdtsc(); // Uncomment this to count in initial idle time.
    uint64_t lastIdleStart = 0;
    uint64_t idleTotal = 0;
    uint64_t workTotal = 0;
    bool printedIdleWarning = false;
    while (context->state < 3) {
        if (context->state != 2) {
            if (Cycles::toSeconds(Cycles::rdtsc() - lastIdleStart) > 1 && !printedIdleWarning && lastIdleStart) {
                printf("WARNING: WorkerThread has been idle more than 1 sec. current state: %d\n", context->state.load());
                printedIdleWarning = true;
            }
            continue;
        }
        
        if (lastIdleStart) { // Exclude initial idle time.
            idleTotal += Cycles::rdtsc() - lastIdleStart;
        }
        uint64_t workStart = Cycles::rdtsc();
        printedIdleWarning = false;
        
        
        std::vector<MillisortArrayPtr>& arrays = context->arrays;
//        printf("Input:  ");
//        for (int i = 0; i < arrays.size(); i++) {
//            printData(arrays[i]);
//        }
//        printf("\n");

        int destIdx = 0;
        
        struct PqItem {
            char* key;
            int arrayId;
            PqItem& operator=(PqItem other)
            {
                key = other.key;
                arrayId = other.arrayId;
                return *this;
            }
        };
        class PqComparator {
        public:
            bool operator() (const PqItem& w1, const PqItem& w2)
            {
                return std::memcmp(w1.key, w2.key, MillisortItem::MillisortKeyLen) > 0;
            }
        };
        std::priority_queue<PqItem, std::vector<PqItem>, PqComparator> pq;
        uint nextIds[arrays.size()];
        bzero(nextIds, sizeof(uint) * arrays.size());
        for (uint i = 0; i < arrays.size(); i++) {
            PqItem item;
            assert(arrays[i].size > 0);
            item.key = arrays[i].data[0].key; // it might be faster to copy key buffer... TODO: test this.
            item.arrayId = i;
#if VERBOSE
            printf("Pushing to PQ. size: %d, newItemKey: %" PRIu64 "\n",
                        pq.size(), bswap_64(*((uint64_t*)item.key)));
#endif
            pq.push(item);

        }

        while (!pq.empty()) {
            PqItem minItem = pq.top();
            MillisortItem& value = arrays[minItem.arrayId].data[nextIds[minItem.arrayId]++];
            context->dest.data[destIdx++] = value; // memcpy might be faster?? TODO: test this.. or change copy constructor.
            pq.pop();
            PqItem nextItem;
            if (nextIds[minItem.arrayId] < arrays[minItem.arrayId].size) {
                nextItem.key = arrays[minItem.arrayId].data[nextIds[minItem.arrayId]].key;
                nextItem.arrayId = minItem.arrayId;

#if VERBOSE
//                printf("Pushing to PQ. size: %d, newItemKey: %" PRIu64 "\n",
//                        pq.size(), bswap_64(*((uint64_t*)nextItem.key)));
#endif
                pq.push(nextItem);

            }
        }
        
//        printData(context->dest, "Output: ");
//        printf("\n");
        
        context->state = 1; // Done working.
        workTotal += Cycles::rdtsc() - workStart;
        lastIdleStart = Cycles::rdtsc();
    }
    printf("**** Worker thread exits.. **** Worker Utilization: %.2f (wait time: %.2f ms)\n",
            static_cast<double>(workTotal) / static_cast<double>(workTotal + idleTotal),
            Cycles::toSeconds(idleTotal) * 1000);
}

// Listing 1
// _end pointer point not to the last element, but one past and never access it.
template< class _Type >
inline void merge_ptr(const _Type* a_start, const _Type* a_end, const _Type* b_start, const _Type* b_end, _Type* dst)
{
//    _Type* initial_dst = dst;
//    const _Type* a_start_init = a_start;
//    const _Type* b_start_init = b_start;
//    size_t a_size = a_end - a_start;
//    size_t b_size = b_end - b_start;
    while (a_start < a_end && b_start < b_end) {
//        bool from_a;
        if (*a_start > *b_start) {
            *dst++ = *b_start++;
//            from_a = true;
        } else {
            *dst++ = *a_start++;	// if elements are equal, then a[] element is output
//            from_a = false;
        }
//        if (initial_dst <= (dst-2) && *(dst-2) > *(dst-1)) {
//            printf("reversed order while merging. merged from a? %s | ", from_a ? "yes" : "no");
//            printData(dst-2, 2); printData(a_start_init, a_size, "A: "); printData(b_start_init, b_size, "B: ");
//            printf("\n");
//        }
//        assert(initial_dst > (dst-2) || !(*(dst-2) > *(dst-1)));
    }
    while (a_start < a_end)	*dst++ = *a_start++;
    while (b_start < b_end)	*dst++ = *b_start++;
}

//template< class _Type >
void mergeWorker2(MergeWorkerContext* context) {
//    uint64_t lastIdleStart = Cycles::rdtsc(); // Uncomment this to count in initial idle time.
    uint64_t lastIdleStart = 0;
    uint64_t idleTotal = 0;
    uint64_t workTotal = 0;
    bool printedIdleWarning = false;
    while (context->state < 3) {
        if (context->state != 2) {
            if (Cycles::toSeconds(Cycles::rdtsc() - lastIdleStart) > 1 && !printedIdleWarning && lastIdleStart) {
                printf("WARNING: WorkerThread has been idle more than 1 sec. current state: %d\n", context->state.load());
                printedIdleWarning = true;
            }
            continue;
        }
        
        if (lastIdleStart) { // Exclude initial idle time.
            idleTotal += Cycles::rdtsc() - lastIdleStart;
        }
        uint64_t workStart = Cycles::rdtsc();
        printedIdleWarning = false;
        
        
        std::vector<MillisortArrayPtr>& arrays = context->arrays;
//        printf("Input:  ");
//        for (int i = 0; i < arrays.size(); i++) {
//            printData(arrays[i]);
//        }
//        printf("\n");
        
//        assert(arrays.size() == 2);
//        merge_ptr(arrays[0].data, arrays[0].data + arrays[0].size,
//                arrays[1].data, arrays[1].data + arrays[1].size,
//                context->dest.data);
        
        // TODO: remove this restriction..
        assert(arrays.size() > 0 && ((arrays.size() & (arrays.size() - 1)) == 0)); // checks it's power of 2.

        // Merge multiple arrays (power of 2) with multi-level 2-way merges.
        size_t totalSize = 0;
        for (auto& array : arrays) {
            totalSize += array.size;
        }
        MillisortItem temp[2][totalSize];
        bzero(temp, 2 * totalSize * sizeof(MillisortItem));
        int numLevels = static_cast<int>(ceil(log(double(arrays.size())) / log(2)));
        for (int level = 0; level < numLevels; level++) {
            MillisortItem* dest;
            if (level == numLevels - 1) {
                dest = context->dest.data;
            } else {
                dest = temp[level & 1];
            }
            MillisortItem* bufferStart = arrays[0].data;
            for (int i = 0; i < double(arrays.size()) / pow(2, level + 1); i++) {
                int a = i * 2;
                int b = i * 2 + 1;
                size_t destOffset = arrays[a].data - bufferStart;

//                printf("i: %d, Merging a: %d, a_dist: %d, a_size: %d, b: %d, b_dist: %d, b_size: %d    ",
//                        i, a, arrays[a].data - bufferStart, arrays[a].size,
//                        b, arrays[b].data - bufferStart, arrays[b].size);
                merge_ptr(arrays[a].data, arrays[a].data + arrays[a].size,
                        arrays[b].data, arrays[b].data + arrays[b].size,
                        dest + destOffset);
                arrays[i].data = dest + destOffset;
                arrays[i].size = arrays[a].size + arrays[b].size;
//                printData(dest, totalSize, "ML: ");
//                printf("\n");
            }
//            printData(dest, totalSize, "ML: ");
//            printf("\n");
        }


//        printData(context->dest, "Output: ");
//        printf("\n");
        
        context->state = 1; // Done working.
        workTotal += Cycles::rdtsc() - workStart;
        lastIdleStart = Cycles::rdtsc();
    }
//    if (lastIdleStart) { // Exclude initial idle time.
//        idleTotal += Cycles::rdtsc() - lastIdleStart;
//    }
    printf("**** Worker thread exits.. **** Worker Utilization: %.2f (work time: %.2f ms, wait time: %.2f ms)\n",
            static_cast<double>(workTotal) / static_cast<double>(workTotal + idleTotal),
            Cycles::toSeconds(workTotal) * 1000, Cycles::toSeconds(idleTotal) * 1000);
}


double mergeParallel()
{
    int numMasters = 1024;
    int itemsPerNode = 50;
//    int ways = 2; // Merge ways.
    int maxWays = 2; // Merge ways.
    
//    int numMasters = 32;
//    int itemsPerNode = 1;
////    int maxWays = 16; // Merge ways.
//    int maxWays = 8; // Merge ways.
  

//    int usecToReceiveMsg = 0.7;
    bool feedRealistic = false;
    
    int numAllItems = numMasters * itemsPerNode;
    
    const int numThreads = 7;
    
    // Standard way...
//    int numLevels = static_cast<int>(ceil(log(numMasters) / log(ways)));
//    printf("STARTING merge... numLevels: %d, %f\n", numLevels, log(numMasters) / log(ways));
    std::vector<int> waysList;
    std::vector<int> numArraysTargets;
    int arrayCount = numMasters;
    while (arrayCount > 1) {
        numArraysTargets.push_back(arrayCount);
        if (arrayCount / numThreads > maxWays) {
            waysList.push_back(maxWays);
            arrayCount /= maxWays;
        } else {
            waysList.push_back(2);
            arrayCount /= 2;
        }
        printf("<%d, %d> ", waysList.back(), numArraysTargets.back());
    }
    printf(" total levels: %lu\n", waysList.size());
    int numLevels = int(waysList.size());
    
    ////////////////////////////////////////////////
    // Prepare input.
    ////////////////////////////////////////////////
//    uint64_t keys[numAllItems] = {461, 732, 276, 919, 725, 551, 704, 626, 494, 247, 348, 202, 898, 153, 420, 912, 134, 159, 755, 422, 432, 309, 604, 853, 777, 302, 5, 532, 567, 410, 706, 330};
//    bool usePresetKeys = true;
    MillisortItem input[numAllItems];
    for (int i = 0; i < numAllItems; i++) {
        MillisortItem item;        
        fillRandom(item.key, MillisortItem::MillisortKeyLen);
        fillRandom(item.data, MillisortItem::MillisortValueLen);
#if VERBOSE
        uint64_t randKey = generateRandom() % 1000;
//        if (usePresetKeys)
//            randKey = keys[i];
        *((uint64_t*)item.key) = bswap_64(randKey);
        printf("%" PRIu64 ", ", randKey);
#endif
        input[i] = item;
        
        
    }
    printf("\n");

    std::vector<MillisortArrayPtr> initialArrays;
    for (int i = 0; i < numMasters; i++) {
        MillisortArrayPtr ptr;
        ptr.data = input + i * itemsPerNode;
        ptr.size = itemsPerNode;
        initialArrays.push_back(ptr);
        std::sort(ptr.data, ptr.data + ptr.size);
#if VERBOSE
//        printf(" After sort:    ");
//        printData(ptr);
//        printf("\n");
#endif
    }
    
    ////////////////////////////////////////////////
    // Merge controller starts here.
    ////////////////////////////////////////////////
    MergeWorkerContext contexts[numThreads];
    std::thread *threads[numThreads];
    for (int i = 0; i < numThreads; i++) {
        contexts[i].state = 0;
        threads[i] = new std::thread(mergeWorker2, &contexts[i]);
    }

    MillisortItem* bufStorage = new MillisortItem[(numLevels + 1) * numAllItems];
    MillisortItem* buffer[numLevels + 1];
    MillisortItem* addr = bufStorage;
    for (auto& row : buffer) {
        row = addr;
        addr += numAllItems;
    }

    std::queue<MillisortArrayPtr> arraysToMerge[numLevels + 1];
    int numArraysCompleted[numLevels];
    size_t nextAvailable[numLevels + 1] = {};
    bzero(bufStorage, sizeof(MillisortItem) * (numLevels + 1) * numAllItems);
    bzero(numArraysCompleted, sizeof(int) * numLevels);
    
    if (!feedRealistic) {
        for (int i = 0; i < numAllItems; i++) {
            buffer[0][i] = input[i];
        }
        for (int i = 0; i < numMasters; i++) {
            MillisortArrayPtr ptr = initialArrays[i];
            ptr.data = ptr.data - input + buffer[0];
            arraysToMerge[0].push(ptr);
        }
    }
    
    // Start timer... and merge.
    uint64_t start = Cycles::rdtscp();
    uint64_t lastLevelEndTime = start;
    uint64_t lastDebugInfoPrint = Cycles::rdtsc();
//    for (int level = 0; pow(ways, level) < numMasters; level++) {
    for (int level = 0; level < numLevels; level++) {
        // Primary goal is completing this level. However, still work on next
        // level jobs if threads are idle.
#if VERBOSE
        printf(" buffer[%d]: ", level);
        MillisortArrayPtr bufferPtr;
        bufferPtr.data = buffer[level];
        bufferPtr.size = numAllItems;
        printData(bufferPtr);
        printf("\n");
#endif
        int ways = waysList[level];
        int numArraysTarget = numArraysTargets[level];

//        int numArraysTarget = numMasters / pow(ways, level);
        
        while (1) {
            if (numArraysCompleted[level] >= numArraysTarget) {
                break;
            }
            
            if (Cycles::toSeconds(Cycles::rdtsc() - lastDebugInfoPrint) > 1) {
                printf("Level%d numArraysTarget: %d, completed: %d\n", level, numArraysTarget, numArraysCompleted[level]);
                lastDebugInfoPrint = Cycles::rdtsc();
            }
            
            // Simulate receiving messages from other servers.
            if (feedRealistic && level == 0) {
                uint64_t currentTime = Cycles::rdtsc();
                _unused(currentTime);
                //TODO: implement!
            }
            
            // Sweep through threads.
            for (int tid = 0; tid < numThreads; tid++) {
                if (contexts[tid].state == 1) { // Done working.
                    // BUG BUG BUG! if the thread was ran for upper level!
                    // Put the level info to the thread context.
//                    printf("Level%d thread %d done.\n", level, tid);
                    numArraysCompleted[level] += int(contexts[tid].arrays.size());
                    arraysToMerge[level + 1].push(contexts[tid].dest);
#if VERBOSE
                    printf("arraysToMerge[%d].size = %d. Inserted merged array size of %d\n",
                            level + 1, arraysToMerge[level + 1].size(), contexts[tid].dest.size);
#endif
                    
                    contexts[tid].state = 0;
                    
#if VERBOSE
                    printf(" Entire buffer: ");
                    MillisortArrayPtr bufferPtr;
                    bufferPtr.data = buffer[level + 1];
                    bufferPtr.size = numAllItems;
                    printData(bufferPtr);
                    printf("\n");
#endif
                }
                if (contexts[tid].state == 0) {
                    if (int(arraysToMerge[level].size()) >= ways ||
                            (arraysToMerge[level].size() > 0 &&
                            int(arraysToMerge[level].size()) + numArraysCompleted[level] == numArraysTarget)) {
                        // Schedule work for the current level.
                        contexts[tid].arrays.clear();
                        contexts[tid].arrays.reserve(ways);
                        int afterMergeSize = 0;
                        for (int i = 0;
                                i < ways && arraysToMerge[level].size() > 0;
                                i++) {
                            contexts[tid].arrays.push_back(arraysToMerge[level].front());
                            afterMergeSize += int(arraysToMerge[level].front().size);
                            arraysToMerge[level].pop();
                        }
                        contexts[tid].dest.data = buffer[level + 1] + nextAvailable[level + 1];
                        contexts[tid].dest.size = afterMergeSize;
                        nextAvailable[level + 1] += afterMergeSize;
//                        printf("Level%d tid%d scheduled merging %d arrays for current level. afterMergeSize: %d, completed: %d out of %d\n",
//                                level, tid, contexts[tid].arrays.size(), afterMergeSize, numArraysCompleted[level], numArraysTarget);
//                    } else if (arraysToMerge[level + 1].size() >= ways) {
//                        // printf("Level%d thread %d scheduling for upper level.\n", level, tid);
//                        // Since we couldn't find enough work for current level,
//                        // work for next level.
//                        // TODO: implement!
//                        continue;
                    } else {
                        // Not enough work to be scheduled. Leave it idle.
                        // Measure time?
                        continue;
                    }
//                    printf("Level%d thread %d scheduled a job.\n", level, tid);
                    contexts[tid].state = 2;
                }
            }
        }
        uint64_t currentTime = Cycles::rdtscp();
        printf("Level %d done. So far %.2f ms (+ %.2f ms) has been elapsed.\n", level,
                Cycles::toSeconds(currentTime - start) * 1000,
                Cycles::toSeconds(currentTime - lastLevelEndTime) * 1000);
        lastLevelEndTime = currentTime;
    }
    uint64_t stop = Cycles::rdtscp();
    
    // Join threads.
    for (int i = 0; i < numThreads; i++) {
        contexts[i].state = 3; // poison the thread to stop.
        threads[i]->join();
        delete threads[i];
    }
    

    // Final check for correctness.
    assert(arraysToMerge[numLevels].size() == 1);
    assert(int(arraysToMerge[numLevels].front().size) == numAllItems);
    MillisortItem inputSorted[numAllItems];
    memcpy(inputSorted, input, sizeof(MillisortItem) * numAllItems);
    std::sort(inputSorted, inputSorted + numAllItems);
    
    for (int i = 0; i < numAllItems; i++) {
        if (memcmp(inputSorted[i].key, buffer[numLevels][i].key,
                MillisortItem::MillisortKeyLen) != 0) {
            printf("!! Non-match found at %d, value: %" PRIu64 " and %" PRIu64"\n",
                    i, bswap_64(*((uint64_t*)inputSorted[i].key)), bswap_64(*((uint64_t*)buffer[numLevels][i].key)));
            
            MillisortArrayPtr ptr;
            ptr.data = buffer[numLevels];
            ptr.size = numAllItems;
            printData(ptr);
            printf("\n");
            ptr.data = inputSorted;
            ptr.size = numAllItems;
            printData(ptr);
            printf("\n");
            exit(1);
        }
        assert(memcmp(inputSorted[i].key, buffer[numLevels][i].key,
                MillisortItem::MillisortKeyLen) == 0);
    }

    printf("Finished merging %d items (%d from %d sources) with %d workerCores.\n",
            numAllItems, numMasters, itemsPerNode, numThreads);
    return Cycles::toSeconds(stop - start);
}

double sortStd()
{
    int count = 100;
    uint64_t totalTime = 0;
    for (int expId = 0; expId < count; expId++) {
        std::vector<MillisortItem> items;
        int itemCount = 50 * 1000;
        for (int i = 0; i < itemCount; i++) {
            MillisortItem item;
            fillRandom(item.key, MillisortItem::MillisortKeyLen);
            fillRandom(item.data, MillisortItem::MillisortValueLen);
            items.push_back(item);
        }

        uint64_t start = Cycles::rdtscp();
        std::sort(items.begin(), items.end());
        uint64_t stop = Cycles::rdtscp();
        totalTime += stop - start;
    }

    return Cycles::toSeconds(totalTime) / count;
}

double mergeModule()
{
    int count = 100;
    uint64_t totalTime = 0;
    int numMasters = 1024;
    int numItemsPerNode = 50;
    for (int expId = 0; expId < count; expId++) {
        Merge<MillisortItem> merger(numMasters, numItemsPerNode * numMasters * 2);
        MergeTestTools<MillisortItem> testSetup;
        testSetup.initializeInput(numItemsPerNode, numMasters);
        merger.prepareThreads();
        
        // Run merge. First add all input arrays. Then keep polling to finish.
        for (auto array : testSetup.initialArrays) {
            merger.poll(array.data, array.size);
            // We may add intentional delays here to simulate online merge.
        }
        while (merger.poll());

        totalTime += merger.stopTick - merger.startTick;
        
        // verify output
        auto mergedArray = merger.getSortedArray();
        testSetup.verifyOutput(mergedArray.data, mergedArray.size);
        if (expId == count - 1) {
            merger.getPerfStats()->printStats();
        }
    }

    return Cycles::toSeconds(totalTime) / count;
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the performance test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    double (*func)();             // Function that implements the test;
                                  // returns the time (in seconds) for each
                                  // iteration of that test.
    const char *description;      // Short description of this test (not more
                                  // than about 40 characters, so the entire
                                  // test output fits on a single line).
};
TestInfo tests[] = {
    {"memcmp10b", memcmp10b,
     "memcmp two randomly filled 10b memory buffers."},
    {"memcpy16b", memcpy16b,
     "memcpy a random 16b memory buffers."},
    {"mergeAllData", mergeAllData,
     "merge all millisort data."}, 
    {"sortStd", sortStd,
     "sort 50k items with std::sort."},
    {"mergeStrategy", mergeStrategy,
     "merge 50k items."},
    {"mergeParallel", mergeParallel,
     "merge 50k items using multiple cores"},
    {"mergeModule", mergeModule,
     "merge 50k items using multiple cores using Merge class"}
//    {"sortSka", sortSka,
//     "sort 50k items with ska_sort, which is a single core Radix sort."}, 
};

/**
 * Runs a particular test and prints a one-line result message.
 *
 * \param info
 *      Describes the test to run.
 */
void runTest(TestInfo& info)
{
    double secs = info.func();
    int width = printf("%-23s ", info.name);
    if (secs < 1.0e-06) {
        width += printf("%8.2fns", 1e09*secs);
    } else if (secs < 1.0e-03) {
        width += printf("%8.2fus", 1e06*secs);
    } else if (secs < 1.0) {
        width += printf("%8.2fms", 1e03*secs);
    } else {
        width += printf("%8.2fs", secs);
    }
    printf("%*s %s\n", 26-width, "", info.description);
}

int
main(int argc, char *argv[])
{
//    // Increase stack size.
//    const rlim_t kStackSize = 8 * 1024 * 1024;   // min stack size = 16 MB
//    struct rlimit rl;
//    int result = getrlimit(RLIMIT_STACK, &rl);
//    if (result == 0) {
//        if (rl.rlim_cur < kStackSize) {
//            rl.rlim_cur = kStackSize;
//            result = setrlimit(RLIMIT_STACK, &rl);
//            if (result != 0) {
//                fprintf(stderr, "setrlimit returned result = %d\n", result);
//            }
//        }
//    }
    
    Cycles::init();
    // Parse command-line options.
    namespace po = boost::program_options;
    vector<string> testNames;
    po::options_description optionsDesc(
            "Usage: Perf [options] testName testName ...\n\n"
            "Runs one or more micro-benchmarks and outputs performance "
            "information.\n\n"
            "Allowed options");
    optionsDesc.add_options()
        ("help", "Print this help message and exit")
        // ("secondCore", po::value<int>(&core2)->default_value(3),
        //         "Second core to use for tests involving two cores (first "
        //         "core is always 2)")
        ("testName", po::value<vector<string>>(&testNames),
                "A test is run if its name contains any of the testName"
                "arguments as a substring");

    po::positional_options_description posDesc;
    posDesc.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
          options(optionsDesc).positional(posDesc).run(), vm);
    po::notify(vm);
    if (vm.count("help")) {
        // std::cout << optionsDesc << "\n";
        exit(0);
    }

    // CPU_ZERO(&savedAffinities);
    // sched_getaffinity(0, sizeof(savedAffinities), &savedAffinities);
    if (argc == 1) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            runTest(info);
        }
    } else {
        // Run only the tests whose names contain at least one of the
        // command-line arguments as a substring.
        bool foundTest = false;
        foreach (TestInfo& info, tests) {
            for (size_t i = 0; i < testNames.size(); i++) {
                if (strstr(info.name, testNames[i].c_str()) !=  NULL) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
        }
        if (!foundTest) {
            printf("No tests matched given arguments\n");
        }
    }
}
