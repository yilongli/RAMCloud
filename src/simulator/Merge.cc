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

#include "Merge.h"

#include <algorithm>
#include <cmath>
#include <byteswap.h>

#include "Common.h"
#include "Cycles.h"

#include "MilliSortService.h"
#include "Arachne/DefaultCorePolicy.h"

using namespace RAMCloud;
//#define VERBOSE 1

/////////////////////////////////////////////
// Merge policy
/////////////////////////////////////////////
#define SPLIT_MERGE 1
#define STATIC_SPLIT 1
#define RAND_INIT_ARR_SIZE 0

void
MergeStats::calcSecondaryStats()
{
    for (int tid = 0; tid < numWorkers; tid++) {
        busyMsByThread[tid] = 0;
        noJobMsByThread[tid] = 0;
    }
    msTotal = 0;
    for (int l = 0; l < numLevels; l++) {
        msTotal += msByLevel[l];
        for (int tid = 0; tid < numWorkers; tid++) {
            busyMsByThread[tid] += Cycles::toSeconds(busyCyclesByThreadAndLevel[tid][l]) * 1000;
            noJobMsByThread[tid] += Cycles::toSeconds(noJobCyclesByThreadAndLevel[tid][l]) * 1000;
        }
    }
}

void
MergeStats::printStats()
{
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf("=== Experiment Info.\n");
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf(" - # of nodes (source arrays): %d\n", numMasters);
    printf(" - Total # of items: %d\n", totalItemCounts);
    // This bytes for key and value won't work correctly if T is not MillisortItem..
    printf(" - Item size: %d (%d B key + %d B value)\n", itemSize,
            MillisortItem::KeyLength, MillisortItem::ValueLength);
    printf("\n");
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf("=== Configuration Info.\n");
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf(" - Split big merge jobs: %s\n", (SPLIT_MERGE) ? "YES" : "NO");
    printf(" - Batched dispatch: %s (maxWays: %d)\n",
            (maxWays > 2) ? "YES" : "NO", maxWays);
    printf("\n");

    calcSecondaryStats();
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf("=== Per thread statistics.\n");
    printf("============================================================="
            "============================================================"
            "===========================\n");
    printf("Level  Total  ");
    for (int tid = 0; tid < numWorkers; tid++) {
        printf("      thread%2d     ", tid);
    }
    printf(" | Per-item busy | Scheduled\n");
    printf("        (ms)  ");
    for (int tid = 0; tid < numWorkers; tid++) {
        printf(" busy/noJob/trnAro ");
    }
    printf(" | (ns);(min,max)| Normal  Split(#job)\n");
    printf("-------------------------------------------------------------"
            "------------------------------------------------------------"
            "---------------------------|---------------|---------------\n");

    for (int l = 0; l < numLevels; l++) {
        printf("   %d   %4.2f :", l, msByLevel[l]);
        double totalBusyTime = 0;
        uint64_t totalItemCount = 0;
        double perItemTimeMax = 0, perItemTimeMin = 9999;
        int perItemTimeMaxTid = -1, perItemTimeMinTid = -1;
        int internalLevels = static_cast<int>(ceil(log(waysList[l]) / log(2)));
        for (int tid = 0; tid < numWorkers; tid++) {
            double busyCycles = Cycles::toSeconds(
                    busyCyclesByThreadAndLevel[tid][l]) * 1000;
            double noJobCycles = Cycles::toSeconds(
                    noJobCyclesByThreadAndLevel[tid][l]) * 1000;
            printf("  %5.2f/%5.2f/%5.2f",
                    busyCycles, noJobCycles,
                    msByLevel[l] - busyCycles - noJobCycles);
            
            double busyNs = Cycles::toSeconds(busyCyclesByThreadAndLevel[tid][l]) * 1e9;
            totalBusyTime += busyNs;
            totalItemCount += jobSizeByThreadAndLevel[tid][l];
            double perItemTime = busyNs / double(jobSizeByThreadAndLevel[tid][l]) / internalLevels;
            if (perItemTime > perItemTimeMax) { perItemTimeMax = perItemTime; perItemTimeMaxTid = tid;}
            if (perItemTime < perItemTimeMin) { perItemTimeMin = perItemTime; perItemTimeMinTid = tid;}
        }
        printf("  | %3.1f (%2.0f, %2.0f)", totalBusyTime / double(totalItemCount) / internalLevels,
                perItemTimeMin, perItemTimeMax);
        printf(" | %4d  %4d(%d)\n", scheduleNormal[l], scheduleSplit[l],
                scheduleSplitJob[l]);

//        printf("             ");
        printf("     (%2d-way)", waysList[l]);
        for (int tid = 0; tid < numWorkers; tid++) {
            double busyCycles = Cycles::toSeconds(
                    busyCyclesByThreadAndLevel[tid][l]) * 1000;
            double noJobCycles = Cycles::toSeconds(
                    noJobCyclesByThreadAndLevel[tid][l]) * 1000;
            printf("   (%3.0f%%/%3.0f%%/%3.0f%%)",
                    busyCycles / msByLevel[l] * 100,
                    noJobCycles / msByLevel[l] * 100,
                    (msByLevel[l] - busyCycles - noJobCycles) / msByLevel[l] * 100);
        }
        printf("  |       T%d, T%d  |\n", perItemTimeMinTid, perItemTimeMaxTid);
    }
    printf("-------------------------------------------------------------"
        "------------------------------------------------------------"
        "---------------------------|---------------|---------------\n");
    
    printf(" SUM   %2.2f :", msTotal);
    for (int tid = 0; tid < numWorkers; tid++) {
        printf("  %5.2f/%5.2f/%5.2f",
                busyMsByThread[tid], noJobMsByThread[tid],
                msTotal - busyMsByThread[tid] - noJobMsByThread[tid]);
    }
    printf("\n");

    printf("             ");
    for (int tid = 0; tid < numWorkers; tid++) {
        printf("   (%3.0f%%/%3.0f%%/%3.0f%%)",
                busyMsByThread[tid] / msTotal * 100,
                noJobMsByThread[tid] / msTotal * 100,
                (msTotal - busyMsByThread[tid] - noJobMsByThread[tid]) / msTotal * 100);
    }
    printf("\n");
    printf("-------------------------------------------------------------"
        "------------------------------------------------------------"
        "---------------------------\n");
    
    double busyMsTotal = 0;
    double noJobMsTotal = 0;
    for (int tid = 0; tid < numWorkers; tid++) {
        busyMsTotal += busyMsByThread[tid];
        noJobMsTotal += noJobMsByThread[tid];
    }
    printf(" Overall CPU utilization: %5.2f/%5.2f/%5.2f (%3.0f%%/%3.0f%%/%3.0f%%)\n",
            busyMsTotal, noJobMsTotal,
            msTotal * numWorkers - busyMsTotal - noJobMsTotal,
            busyMsTotal / (msTotal * numWorkers) * 100,
            noJobMsTotal / (msTotal * numWorkers) * 100,
            (msTotal * numWorkers - busyMsTotal - noJobMsTotal)
                    / (msTotal * numWorkers) * 100);
    printf("-------------------------------------------------------------"
        "------------------------------------------------------------"
        "---------------------------\n");
}

/**
 * Fill the given address with random data.
 *
 * \param dest
 *      Memory block in which to write in random data.
 * \param length
 *      Total number of bytes to write.
 */
template<class T>
void
MergeTestTools<T>::fillRandom(char* dest, uint32_t length)
{
    memset(dest, 'x', length);
    for (int offset = 0; offset <= static_cast<int>(length) - 8; offset += 8) {
        *(reinterpret_cast<uint64_t*>(dest + offset)) = mcg64();
    }
    int bytesLeftToFill = length & 7;
    for (uint offset = length - bytesLeftToFill; offset < length; offset++) {
        *(dest + offset) = downCast<char>(mcg64() & 127);
    }
}

//typedef Merge<MillisortItem>::ArrayPtr MillisortArrayPtr;
#define MillisortArrayPtr typename Merge<T>::ArrayPtr

template <typename T>
__attribute__((unused))
static void printData(const T* ptr, size_t length, const char* header = "", bool warnIfUnsorted = true) {
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

/**
 * Verifies the merge job just done by a mergeWorker.
 */
template<class T>
void verifyMergeJob(std::vector<typename Merge<T>::ArrayPtr>& arrays, T* dest) {
#ifndef VERBOSE
    printf("DO NOT RUN runtime verifier during benchmark!\n");
    exit(1);
#endif

    std::vector<T> result;
    for (uint i = 0; i < arrays.size(); i++) {
        for (uint j = 0; j < arrays[i].size; j++) {
            result.push_back(arrays[i].data[j]);
        }
    }
    std::sort(result.begin(), result.end());

    for (int i = 0; i < result.size(); i++) {
//        if (std::memcmp(dest[i].key, result[i].key,
//                T::KeyLength) != 0) {
        if (!(dest[i] == result[i])) {
            printf("***** ERROR! mismatch found! at %d ***** Arrays: %d\n",
                    i, arrays.size());
            printData(result.data(), result.size(), "Expected: ");
            printf("\n");
            printData(dest, result.size(), "Actual: ");
            exit(1);
        }
    }
}

template <typename T>
__attribute__((unused))
static void printData(MillisortArrayPtr ptr, const char* header = "", bool warnIfUnsorted = true) {
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

template<class T>
inline static void merge_ptr(const T* a_start, const T* a_end, const T* b_start,
        const T* b_end, T* dst)
{
    while (a_start < a_end && b_start < b_end) {
        if (!(*a_start < *b_start)) {
            *dst++ = *b_start++;
        } else {
            *dst++ = *a_start++;
        }
    }
    while (a_start < a_end) {
        *dst++ = *a_start++;
    }
    while (b_start < b_end) {
        *dst++ = *b_start++;
    }
}

template<class T>
void Merge<T>::mergeWorker(MergeWorkerContext* context) {
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
#if USE_ARACHNE
            Arachne::yield();
#endif
            continue;
        }

        if (lastIdleStart) { // Exclude initial idle time.
            idleTotal += Cycles::rdtsc() - lastIdleStart;
        }
        uint64_t workStart = Cycles::rdtsc();
        printedIdleWarning = false;


//        std::vector<ArrayPtr>& arrays = context->arrays;
        int inputArrayCounts = downCast<int>(context->arrays.size());
        ArrayPtr arrays[context->arrays.size()];
        memcpy(arrays, context->arrays.data(), sizeof(ArrayPtr)*inputArrayCounts);
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

        assert(context->arrays.size() > 0);
        assert((context->arrays.size() & (context->arrays.size() - 1)) == 0); // checks it's power of 2.

        // Merge multiple arrays (power of 2) with multi-level 2-way merges.
        size_t totalSize = 0;
        for (int i = 0; i < inputArrayCounts; i++) {
            totalSize += arrays[i].size;
        }
        T temp[2][totalSize];
#ifdef VERBOSE
        bzero(temp, 2 * totalSize * sizeof(T));
#endif
        int numLevels = static_cast<int>(ceil(log(inputArrayCounts) / log(2)));
        for (int level = 0; level < numLevels; level++) {
            T* dest;
            if (level == numLevels - 1) {
                dest = context->dest.data;
            } else {
                dest = temp[level & 1];
            }

//            T* bufferStart = arrays[0].data;
            size_t destOffset = 0;
            for (int i = 0; i < inputArrayCounts / pow(2, level + 1); i++) {
                int a = i * 2;
                int b = i * 2 + 1;
//                size_t destOffset = arrays[a].data - bufferStart;

                merge_ptr(arrays[a].data, arrays[a].data + arrays[a].size,
                        arrays[b].data, arrays[b].data + arrays[b].size,
                        dest + destOffset);
                arrays[i].data = dest + destOffset;
                destOffset += arrays[a].size + arrays[b].size;
                arrays[i].size = arrays[a].size + arrays[b].size;

//                printData(dest, totalSize, "ML: ");
//                printf("\n");
            }
//            printData(dest, totalSize, "ML: ");
//            printf("\n");
        }


//        printData(context->dest, "Output: ");
//        printf("\n");
        uint64_t finishTime = Cycles::rdtsc();
        uint64_t currentJobCycles = finishTime - workStart;
        context->currentJobCycles = currentJobCycles;
        context->lastIdleTime = finishTime;
        context->state = 1; // Done working.

        workTotal += currentJobCycles;
        lastIdleStart = finishTime;
    }
//    if (lastIdleStart) { // Exclude initial idle time.
//        idleTotal += Cycles::rdtsc() - lastIdleStart;
//    }
//    printf("**** Worker thread exits.. **** Worker Utilization: %.2f (work time: %.2f ms, wait time: %.2f ms)\n",
//            static_cast<double>(workTotal) / static_cast<double>(workTotal + idleTotal),
//            Cycles::toSeconds(workTotal) * 1000, Cycles::toSeconds(idleTotal) * 1000);
}

template<class T>
Merge<T>::Merge(int numArraysTotal, int maxNumAllItems, int numWorkers)
    : initialArraysToMergeCounts(numArraysTotal)
    , numAllItems(maxNumAllItems)
    , numWorkers(numWorkers)
    , numArraysCompleted()
    , startTime(0)
{
    int nextPowerOfTwo = pow(2, ceil(log(numArraysTotal)/log(2)));
    // Fake the total array count to be a power of 2 by adding dummies.
    for (int i = 0; i < nextPowerOfTwo - numArraysTotal; ++i) {
        ArrayPtr ptr;
        ptr.data = 0;
        ptr.size = 0;
        arraysToMerge[0].push(ptr);
    }

    int arrayCount = nextPowerOfTwo;
    while (arrayCount > 1) {
        numArraysTargets.push_back(arrayCount);
        if (arrayCount / numWorkers > maxWays) {
            waysList.push_back(maxWays);
            arrayCount /= maxWays;
        } else {
            waysList.push_back(2);
            arrayCount /= 2;
        }
#ifdef VERBOSE
        printf("<%d, %d> ", waysList.back(), numArraysTargets.back());
#endif
    }
#ifdef VERBOSE
    printf(" total levels: %d\n", waysList.size());
#endif
    int numLevels = int(waysList.size());
    buffer = new T[(numLevels + 1) * numAllItems];
    bzero(buffer, sizeof(T) * (numLevels + 1) * numAllItems);

    perfStats.initialize(numLevels, numWorkers);
}

template<class T>
Merge<T>::~Merge()
{
    // Join threads.
    for (int i = 0; i < numWorkers; i++) {
        contexts[i].state = 3; // poison the thread to stop.
#if USE_ARACHNE
        // FIXME: shouldn't be necessary once Arachne::join is fixed.
        if (threads[i].context == NULL) continue;
        Arachne::join(threads[i]);
#else
        threads[i]->join();
        delete threads[i];
#endif
    }
    delete[] buffer;
}

template<class T>
void Merge<T>::prepareThreads()
{
#if USE_ARACHNE
    Arachne::CorePolicy::CoreList list = Arachne::getCorePolicy()->getCores(0);
#endif
    for (int i = 0; i < numWorkers; i++) {
        contexts[i].state = 0;
#if USE_ARACHNE
        // FIXME: need to take into account dispatcher?
        // FIXME: Or, shall we even prepare threads on startup? this seems to be
        // against Arachne's model; shall we create Arachne thread in poll()
        // by need?
        // Manually spread out the workers on all available cores.
        uint32_t coreId = downCast<uint32_t>(list[i % list.size()]);
        threads[i] = Arachne::createThreadOnCore(coreId, MERGE_WORKER,
                &contexts[i]);
//        LOG(DEBUG, "spawn worker %d on core %u", i, threads[i].context->originalCoreId);
//        threads[i] = Arachne::createThreadWithClass(
//                Arachne::DefaultCorePolicy::EXCLUSIVE, MERGE_WORKER,
//                &contexts[i]);
#else
        threads[i] = new std::thread(MERGE_WORKER, &contexts[i]);
#endif
    }
    preparedThreads = true;
}

template<class T>
inline size_t
binarySearch(T* data, size_t size, const T& key)
{
    size_t low  = 0;
    size_t high = size;
    while( low < high )
    {
        size_t mid = ( low + high ) / 2;
        if (data[mid] < key)
            low = mid + 1;
        else
            high = mid;
    }
    return high;
}

template<class T>
void
Merge<T>::scheduleSplitted(SplittedMergeJob job, int tid)
{
    assert(contexts[tid].state == 0);
    contexts[tid].arrays.clear();
    contexts[tid].arrays.push_back(job.src1);
    contexts[tid].arrays.push_back(job.src2);
    contexts[tid].dest = job.dest;
    contexts[tid].isSplittedSubJob = job.isSubJob;

    contexts[tid].currentLevel = level;
    contexts[tid].state = 2;
    perfStats.jobSizeByThreadAndLevel[tid][level] +=
            job.src1.size + job.src2.size;
}

/**
 * Proceed merging.
 *
 * \return  true if more jobs need to be done.
 *          false if merge is completed and no more poll() is necessary.
 */
template<class T>
bool
Merge<T>::poll()
{
    assert(preparedThreads);
    if (!isStarted) {
        isStarted = true;
        startTick = Cycles::rdtscp();
        lastLevelEndTime = startTick;
        lastDebugInfoPrint = startTick;
        level = 0;
#ifdef VERBOSE
        printf("Starting merge engine.\n");
#endif
    }
    
    int ways = waysList[level];
    int numLevels = int(waysList.size());
    int numArraysTarget = numArraysTargets[level];
    
    if (numArraysCompleted[level] == numArraysTarget && splittedJobs.empty()) {
        // If some worker is still working on some part of splitted array,
        // continue polling instead of completing the sort.
        for (int tid = 0; tid < numWorkers; tid++) {
            if (contexts[tid].state > 0) {
                goto continuePolling;
            }
        }
        
        uint64_t currentTick = Cycles::rdtsc();
//        printf("Level %d done. So far %.2f ms (+ %.2f ms) has been elapsed.\n", level,
//                Cycles::toSeconds(currentTick - startTick) * 1000,
//                Cycles::toSeconds(currentTick - lastLevelEndTime) * 1000);
        perfStats.msByLevel[level] = Cycles::toSeconds(currentTick - lastLevelEndTime) * 1000;
        
        for (int tid = 0; tid < numWorkers; tid++) {
            if (contexts[tid].currentLevel == level) {
                // This thread has been used for this level.
                perfStats.noJobCyclesByThreadAndLevel[tid][level]
                        = currentTick - contexts[tid].lastIdleTime;
            } else if (contexts[tid].currentLevel < level) {
                // This thread was not used for this level.
                perfStats.noJobCyclesByThreadAndLevel[tid][level]
                        = currentTick - lastLevelEndTime;
            } else {
                assert(false);
                exit(1);
            }
        }
        
        lastLevelEndTime = currentTick;
        level++;
        if (level >= numLevels) {
            // Merge is completed!
            stopTick = Cycles::rdtscp();
#ifdef VERBOSE
            printf("Finished merging with %d workerCores.\n", numWorkers);
#endif
            return false;
        }

#if VERBOSE
        printf(" buffer[%d]: ", level);
        MillisortArrayPtr bufferPtr;
        bufferPtr.data = &buffer[level * numAllItems];
        bufferPtr.size = numAllItems / 2;
        printData(bufferPtr);
        printf("\n");
#endif
        // go to begining?
        return poll();
    }
    
continuePolling:

    if (Cycles::toSeconds(Cycles::rdtsc() - lastDebugInfoPrint) > 1) {
        printf("Level%d numArraysTarget: %d, completed: %d\n", level, numArraysTarget, numArraysCompleted[level]);
        lastDebugInfoPrint = Cycles::rdtsc();
    }

    // Sweep through threads.
    std::vector<int> idleWorkers;
    // Scan in reverse order for better cache behavior.
    for (int tid = numWorkers - 1; tid >= 0; tid--) {
        if (contexts[tid].state == 1) { // Done working.
//                    printf("Level%d thread %d done.\n", mergeLevel, tid);
            int mergeLevel = contexts[tid].currentLevel;
//            if (contexts[tid].dest.size > 0) {
            if (!contexts[tid].isSplittedSubJob) {
                numArraysCompleted[mergeLevel] += int(contexts[tid].arrays.size());
                arraysToMerge[mergeLevel + 1].push(contexts[tid].dest);
            }

            perfStats.busyCyclesByThreadAndLevel[tid][mergeLevel] +=
                    contexts[tid].currentJobCycles;
#if VERBOSE
            printf("arraysToMerge[%d].size = %d. Inserted merged array size of %d\n",
                    mergeLevel + 1, arraysToMerge[mergeLevel + 1].size(), contexts[tid].dest.size);
#endif

            contexts[tid].state = 0;

#if VERBOSE
            printf(" Entire buffer: ");
            MillisortArrayPtr bufferPtr;
            bufferPtr.data = &buffer[(mergeLevel + 1) * numAllItems];
            bufferPtr.size = numAllItems / 2;
            printData(bufferPtr);
            printf("\n");
            verifyMergeJob<T>(contexts[tid].arrays, contexts[tid].dest.data);
#endif
        }

        if (contexts[tid].state == 0) {
            idleWorkers.push_back(tid);
        }
    }

    while (idleWorkers.size() > 0 &&
            (int(arraysToMerge[level].size()) >= ways ||
            !splittedJobs.empty() ||
            (arraysToMerge[level].size() > 0 &&
                int(arraysToMerge[level].size()) + numArraysCompleted[level] == numArraysTarget))) {
        int jobsAvailable = int(arraysToMerge[level].size()) / ways;
        if (!splittedJobs.empty()) {
            // There is a job that was split but haven't been fully scheduled.
            scheduleSplitted(splittedJobs.front(), idleWorkers.back());
            splittedJobs.pop();
            idleWorkers.pop_back();
        } else if (SPLIT_MERGE && ways == 2 &&
                ((STATIC_SPLIT && numWorkers >= 2)
                    || idleWorkers.size() / jobsAvailable >= 2) &&
                int(arraysToMerge[level].front().size) > numWorkers) {
            // Split two arrays and schedule merge to multiple workers.
#if STATIC_SPLIT
            int splitWays = numWorkers;
#else
            int splitWays = idleWorkers.size() / jobsAvailable;
#endif
            auto first = arraysToMerge[level].front();
            arraysToMerge[level].pop();
            auto second = arraysToMerge[level].front();
            arraysToMerge[level].pop();
#if VERBOSE
            printf(" [%d] Splitting arrays of size %d, %d into %d partitions (%d idle cores), %d jobs available\n",
                    level, first.size, second.size, splitWays, idleWorkers.size(), jobsAvailable);
#endif
            ArrayPtr a;
            a.data = first.data;
            a.size = first.size / splitWays;
            ArrayPtr b;
            b.data = second.data;
            b.size = binarySearch(b.data, second.size, a.data[a.size]);
            
            assert(splitWays >= 2);
            for (int part = 0; part < splitWays; part++) {
                SplittedMergeJob job;
                job.src1 = a;
                job.src2 = b;
                job.dest.data = &buffer[(level + 1) * numAllItems]
                        + nextAvailable[level + 1];
                nextAvailable[level + 1] += a.size + b.size;
                // It is a bit hacky... worker doesn't use this size.
                // Use size 0 to indicate that no need to add it in arraysToSort later.
                job.dest.size = (part == 0) ? (first.size + second.size) : 0;
                job.isSubJob = (part == 0) ? false : true;

                int scheduledTid = -1;
                _unused(scheduledTid);
                if (idleWorkers.empty()) {
                    // No more idle thread, just push it into queue.
                    splittedJobs.push(job);
                } else {
                    scheduledTid = idleWorkers.back();
                    scheduleSplitted(job, idleWorkers.back());
                    idleWorkers.pop_back();
                }
#if VERBOSE
                printf("Level%d SPLIT %s (tid%d). Src: %d, %d Dst: %d\n",
                        level, (scheduledTid >= 0) ? "scheduled" : "no idle thread", scheduledTid,
                        job.src1.data - &buffer[level * numAllItems],
                        job.src2.data - &buffer[level * numAllItems],
                        job.dest.data - &buffer[(level + 1) * numAllItems]);
#endif
                
                // Advance array a and b.
                a.data += a.size;
                b.data += b.size;
                if (part == splitWays - 2) {
                    // Last partition. include everything.
                    a.size = first.data + first.size - a.data;
                    b.size = second.data + second.size - b.data;
                } else {
                    size_t bTotalLeft = second.data + second.size - b.data;
                    b.size = binarySearch(b.data, bTotalLeft, a.data[a.size]);
                }
            }
            perfStats.scheduleSplitJob[level]++;
            perfStats.scheduleSplit[level] += splitWays;
        } else {
            int tid = idleWorkers.back();
            idleWorkers.pop_back();

            contexts[tid].arrays.clear();
            contexts[tid].arrays.reserve(ways);
            int afterMergeSize = 0;
            
            for (int i = 0; i < ways && arraysToMerge[level].size() > 0; i++) {
                contexts[tid].arrays.push_back(arraysToMerge[level].front());
                afterMergeSize += downCast<int>(arraysToMerge[level].front().size);
                arraysToMerge[level].pop();
            }
            contexts[tid].dest.data = &buffer[(level + 1) * numAllItems]
                    + nextAvailable[level + 1];
            contexts[tid].dest.size = afterMergeSize;
            contexts[tid].isSplittedSubJob = false;
            nextAvailable[level + 1] += afterMergeSize;
            contexts[tid].currentLevel = level;
#if VERBOSE
            printf("Level%d tid%d scheduled merging %d arrays for current level. afterMergeSize: %d, completed: %d out of %d\n",
                    level, tid, contexts[tid].arrays.size(), afterMergeSize, numArraysCompleted[level], numArraysTarget);

            int sourceCount = contexts[tid].arrays.size();
            printf("Level%d tid%d scheduled for %d arrays. Src: %d, %d, %d, %d Dst: %d\n",
                    level, tid, contexts[tid].arrays.size(),
                    (sourceCount > 0) ? (contexts[tid].arrays[0].data - &buffer[level * numAllItems]) : -1,
                    (sourceCount > 1) ? (contexts[tid].arrays[1].data - &buffer[level * numAllItems]) : -1,
                    (sourceCount > 2) ? (contexts[tid].arrays[2].data - &buffer[level * numAllItems]) : -1,
                    (sourceCount > 3) ? (contexts[tid].arrays[3].data - &buffer[level * numAllItems]) : -1,
                    contexts[tid].dest.data - &buffer[(level + 1) * numAllItems]);
#endif
            
            contexts[tid].state = 2;
            perfStats.jobSizeByThreadAndLevel[tid][level] += afterMergeSize;
            perfStats.scheduleNormal[level]++;
        }

#if USE_ARACHNE
        Arachne::yield();
#endif
    }

    return true;
}

/**
 * Same as poll(), but it adds a new sorted array into a merge list.
 */
template<class T>
bool
Merge<T>::poll(T* newData, size_t size)
{
    assert(preparedThreads);
    if (startTime == 0) {
        startTime = Cycles::rdtsc();
    }
    
    ArrayPtr ptr;
    ptr.data = newData;
    ptr.size = size;
    arraysToMerge[0].push(ptr);
    return poll();
}

template<class T>
typename Merge<T>::ArrayPtr
Merge<T>::getSortedArray()
{
    return arraysToMerge[waysList.size()].front();
}

template<class T>
MergeStats*
Merge<T>::getPerfStats()
{
    perfStats.numLevels = downCast<int>(waysList.size());
    perfStats.waysList.resize(waysList.size());
    memcpy(perfStats.waysList.data(), waysList.data(), sizeof(int) * waysList.size());
    perfStats.maxWays = maxWays;
    perfStats.numMasters = initialArraysToMergeCounts;
    perfStats.totalItemCounts = downCast<int>(arraysToMerge[waysList.size()].front().size);
    perfStats.itemSize = sizeof(T);

    return &perfStats;
}

template<class T>
void
MergeTestTools<T>::initializeInput(int itemsPerNode, int numMasters)
{
    mcg64State = 1;
    size_t numAllItems = itemsPerNode * numMasters;
    input.reserve(numAllItems);
    for (size_t i = 0; i < numAllItems; i++) {
        T item;
        fillRandom((char*)&item, sizeof(T));
#if VERBOSE
        uint64_t randKey = generateRandom() % 1000;
        *((uint64_t*)item.key) = bswap_64(randKey);
        printf("%" PRIu64 ", ", randKey);
#endif
        input.push_back(item);
    }
#if VERBOSE
    printf("\n");
#endif

//    printf("input[0] = %lu, input[1] = %lu\n", *((uint64_t*)&input[0]), *((uint64_t*)&input[1]));

    for (int i = 0; i < numMasters; i++) {
        MillisortArrayPtr ptr;
        ptr.data = &input[i * itemsPerNode];
        ptr.size = itemsPerNode;
        initialArrays.push_back(ptr);
        std::sort(ptr.data, ptr.data + ptr.size);
#if VERBOSE
//        printf(" After sort:    ");
//        printData(ptr);
//        printf("\n");
#endif
    }
}

template<class T>
void
MergeTestTools<T>::verifyOutput(T* mergedData, size_t mergedSize)
{
    size_t numAllItems = input.size();
    if (numAllItems != mergedSize) {
        printf("Verification failed. numAllItems: %lu, mergedSize: %lu\n",
                numAllItems, mergedSize);
    }
    assert(numAllItems == mergedSize);

    T inputSorted[numAllItems];
    memcpy(inputSorted, input.data(), sizeof(T) * numAllItems);
    std::sort(inputSorted, inputSorted + numAllItems);

    for (size_t i = 0; i < numAllItems; i++) {
        if (!(inputSorted[i] == mergedData[i])) {
            printf("!! Non-match found at %lu\n", i);

//            MillisortArrayPtr ptr;
//            ptr.data = mergedData;
//            ptr.size = numAllItems;
//            printData(ptr);
//            printf("\n");
//            ptr.data = inputSorted;
//            ptr.size = numAllItems;
//            printData(ptr);
//            printf("\n");
            exit(1);
        }
        assert(inputSorted[i] == mergedData[i]);
    }
}

template class MergeTestTools<MillisortItem>;
template class Merge<MillisortItem>;
template class MergeTestTools<MilliSortService::PivotKey>;
template class Merge<MilliSortService::PivotKey>;