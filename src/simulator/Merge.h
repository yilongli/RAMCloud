/* Copyright (c) 2018 Stanford University
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

#ifndef MERGE_H
#define MERGE_H

#include <algorithm>
#include <atomic>
#include <byteswap.h>
#include <sys/resource.h>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <cstring>

#include "Cycles.h"

#define MERGE_WORKER mergeWorker
#define MAX_LEVELS 20
#define NUM_WORKER_THREADS 7
//#define NUM_WORKER_THREADS 4

/**
 * Tracks performance statistics of a merge task.
 */
struct MergeStats {
    ///////////////////////////////////////////////////////////////
    // Some key configurations used during merge.
    ///////////////////////////////////////////////////////////////
    int numLevels = 0;
    std::vector<int> waysList;
    int maxWays;
    int numMasters;
    int totalItemCounts;
    int itemSize;
    int keySize;
    int valueSize;

    ///////////////////////////////////////////////////////////////
    // Performance counters
    ///////////////////////////////////////////////////////////////
    std::vector<double> msByLevel;
    std::vector<uint64_t> busyCyclesByThreadAndLevel[NUM_WORKER_THREADS];
    std::vector<uint64_t> noJobCyclesByThreadAndLevel[NUM_WORKER_THREADS];
    std::vector<uint64_t> jobSizeByThreadAndLevel[NUM_WORKER_THREADS];
    int scheduleSplit[MAX_LEVELS] = {};
    int scheduleSplitJob[MAX_LEVELS] = {};
    int scheduleNormal[MAX_LEVELS] = {};

    ///////////////////////////////////////////////////////////////
    // Summarized/derived statistics
    ///////////////////////////////////////////////////////////////
    double msTotal;
    double busyMsByThread[NUM_WORKER_THREADS];
    double noJobMsByThread[NUM_WORKER_THREADS];

    void initialize(int numLevels) {
        this->numLevels = numLevels;
        msByLevel.resize(numLevels);
        for (int tid = 0; tid < NUM_WORKER_THREADS; tid++) {
            busyCyclesByThreadAndLevel[tid].resize(numLevels);
            noJobCyclesByThreadAndLevel[tid].resize(numLevels);
            jobSizeByThreadAndLevel[tid].resize(numLevels);
            bzero(jobSizeByThreadAndLevel[tid].data(),
                    sizeof(uint64_t) * numLevels);
        }
    }
    
    void printStats();
    void calcSecondaryStats();
};

/**
 * This class implements online parallel merge of Millisort.
 */
template<class T>
class Merge {
public:
    /**
     * Represents a c-style array with length.
     */
    struct ArrayPtr {
        T* data;
        size_t size;
    };
    
    Merge(int numArraysTotal, int maxNumAllItems);
    ~Merge();
    void prepareThreads();
    bool poll();
    bool poll(T* newData, size_t size);
    ArrayPtr getSortedArray();
    MergeStats* getPerfStats();

private:
    /**
     * Per-thread context for each worker.
     * Holds a job that need to be processed, current work state, and
     * performance statistics.
     */
    struct MergeWorkerContext {
        // Variable used for synchronization between dispatcher and worker.
        // 0: idle; dispatch can modify.
        // 1: done; dispatch can read and modify.
        // 2: working; dispatch shouldn't touch. Worker can read/modify.
        // 3: die; poison to exit this worker thread.
        std::atomic<int> state; 

        // Description of merge job. Worker will merge #arrays to #dest.
        std::vector<ArrayPtr> arrays;
        ArrayPtr dest;
        
        // Current level in the big merge tree. Used by dispatch only.
        int currentLevel = 0;
        
        // Time spent for this job. Will be read by dispatch when job is done.
        uint64_t currentJobCycles = 0;

        // State used to calculate performance statistics later.
        uint64_t lastIdleTime = 0;
    };

    static void mergeWorker(MergeWorkerContext* context);

public:
    ///////////////////////////////////////////////////////////////
    // Configurations
    ///////////////////////////////////////////////////////////////
    
    // Maximum merge ways per dispatch. Bigger number allows more batching on
    // dispatch and improves performance. This will be ignored for higher levels
    // to allow more parallelism.
    // THIS MUST BE POWER OF 2.
    const int maxWays = 64;
//    const int maxWays = 32;
    
private:
    ///////////////////////////////////////////////////////////////
    // Variable for initial configuration
    ///////////////////////////////////////////////////////////////
    
    // Number of arrays that are fed (or will be fed) to this merger.
    const int initialArraysToMergeCounts;
    
    // Total number of elements across all arrays that need to be merged.
    const int numAllItems;
    
    // Number of arrays to merge at a time in each level.
    std::vector<int> waysList;
    
    // Total number of merged arrays that need to be generated for next level to
    // complete and advance to the next level.
    std::vector<int> numArraysTargets;
    
    // Number of worker threads.
    static const int numThreads = NUM_WORKER_THREADS;
    
    ///////////////////////////////////////////////////////////////
    // Variable tracking current progress
    ///////////////////////////////////////////////////////////////
    MergeWorkerContext contexts[numThreads];
    std::thread *threads[numThreads];
    
    // Memory chunk that are used to store intermediately merged arrays.
    T* buffer;
    
    // Tracks the position in memory buffer that are available to store
    // the resulted arrays from merging.
    size_t nextAvailable[MAX_LEVELS + 1] = {};
    
    // Tracks arrays that need to be merged in each level.
    std::queue<ArrayPtr> arraysToMerge[MAX_LEVELS + 1];

    // Tracks the count of arrays that are already merged. This counter tracks
    // the resulted arrays in each level (not the count of merged-from arrays).
    int numArraysCompleted[MAX_LEVELS];

    /**
     * If the merger splits the big source arrays into smaller ones for more
     * parallelism, this structure holds the each splitted job.
     *
     * #dest may have size of 0 if this job is not the first partial job of the
     * big regular job.
     */
    struct SplittedMergeJob {
        ArrayPtr src1;
        ArrayPtr src2;
        ArrayPtr dest;
    };
    std::queue<SplittedMergeJob> splittedJobs;
    void scheduleSplitted(SplittedMergeJob job, int tid);
    
    // Tracks the current level of merge.
    // A merge task is split by each level. For example, if we are merging
    // 8 arrays into a single array, we may have total of 3 levels.
    // Level 0: merges 8 arrays into 4 arrays.
    // Level 1: merges 4 arrays into 2 arrays.
    // Level 2: merges 2 arrays into 1 arrays.
    int level;

    // Time
    bool isStarted = false;
    bool preparedThreads = false;
    uint64_t lastLevelEndTime;
    uint64_t lastDebugInfoPrint;
public:
    uint64_t startTick;
    uint64_t stopTick;
    MergeStats perfStats;
};

/**
 * Useful tools to test #merge class. It generates inputs and validate the
 * correctness of the result of a merge.
 */
template<class T>
class MergeTestTools {
public:
    explicit MergeTestTools()
        : mcg64State(1)
    {}

    void initializeInput(int itemsPerNode, int numMasters);
    void verifyOutput(T* mergedData, size_t mergedSize);

    std::vector<T> input;
    std::vector<typename Merge< T >::ArrayPtr > initialArrays;
private:
    //
    uint64_t mcg64()
    {
        return (mcg64State = (164603309694725029ull * mcg64State) %
                             14738995463583502973ull);
    }

    uint64_t mcg64State;

    void fillRandom(char* dest, uint32_t length);
};



//struct MillisortItem {
//   static const int KeyLength = 10;
//   static const int ValueLength = 8;
//   char key[KeyLength];
//   char padding1[6];
//   char data[ValueLength];
//   // char padding2[6];
//   bool operator<(const MillisortItem &other) const
//   {
//       return std::memcmp(key, other.key, KeyLength) < 0;
//   }
//   MillisortItem& operator=(MillisortItem other)
//   {
//       std::memcpy(key, other.key, KeyLength);
//       std::memcpy(data, other.data, ValueLength);
//       return *this;
//   }
//}; static_assert(sizeof(MillisortItem) == 24,
//       "Unexpected padding in MillisortItem");

// **** Aligned naturally.
//struct MillisortItem {
//    static const int KeyLength = 8;
//    static const int ValueLength = 88;
//    char key[KeyLength];
//    char data[ValueLength];
//    bool operator<(const MillisortItem &other) const
//    {
//        return std::memcmp(key, other.key, KeyLength) < 0;
//    }
//    MillisortItem& operator=(MillisortItem other)
//    {
//        std::memcpy(key, other.key, KeyLength);
//        std::memcpy(data, other.data, ValueLength);
//        return *this;
//    }
//} __attribute__((packed));
//static_assert(sizeof(MillisortItem) == 96,
//        "Unexpected padding in MillisortItem");

// **** Aligned and faster comparison.
// struct MillisortItem {
//     static const int KeyLength = 8;
//     static const int ValueLength = 88;
//     char key[KeyLength];
//     char data[ValueLength];
//     bool operator<(const MillisortItem &other) const
//     {
//         return *((const uint64_t*)key) < *((const uint64_t*)other.key);
//     }
//     MillisortItem& operator=(MillisortItem other)
//     {
//         std::memcpy(key, other.key, KeyLength);
//         std::memcpy(data, other.data, ValueLength);
//         return *this;
//     }
// };


/// FOR DEBUGGING....
struct MillisortItem {
    static const int KeyLength = 10;
    static const int ValueLength = 6;
    char key[KeyLength];
    char data[ValueLength];

    MillisortItem()
        : key(), data()
    {}

    inline unsigned __int128
    asUint128() const
    {
        return *((const unsigned __int128*) this);
    }

    bool operator<(const MillisortItem &other) const {
        return asUint128() < other.asUint128();
//        unsigned __int128 mine, yours;
//        *(((int64_t*) &mine) + 1) = bswap_64(*((const int64_t*) this));
//        *(((int64_t*) &mine)) = bswap_64(*(((const int64_t*) this) + 1));
//        *(((int64_t*) &yours) + 1) = bswap_64(*((const int64_t*) &other));
//        *(((int64_t*) &yours)) = bswap_64(*(((const int64_t*) &other) + 1));
//        return mine < yours;
    }

    bool operator==(const MillisortItem& other)
    {
        return asUint128() == other.asUint128();
    }
};
static_assert(sizeof(MillisortItem) == 16, "Unexpected padding in MillisortItem");

#endif  // MERGE_H
