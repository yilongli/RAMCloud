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

#ifndef MILLISORT_MERGE_H
#define MILLISORT_MERGE_H

#include <algorithm>
#include <Arachne/Arachne.h>

#include "Common.h"
#include "Cycles.h"
#include "TimeTrace.h"

namespace RAMCloud {

template <typename T, int N = 4>
struct MergeJob {
    T* srcBegin[N];
    T* srcEnd[N];
    T* dest;

    // No initialization.
    MergeJob() {}
};

/// Per-core performance statistics collected during parallel mergesort.
struct MergeStats {
    uint64_t mergeCycles;
    uint64_t splitCycles;
    uint64_t mergeTasks;
    uint64_t splitTasks;

    MergeStats()
        : mergeCycles(), splitCycles(), mergeTasks(), splitTasks()
    {}
};

/**
 * Find the first element within [begin, end) that is greater than or equal to
 * a specified value.
 *
 * \tparam T
 *      Type of the element.
 * \param begin
 *      Left inclusive bound the elements to search from.
 * \param end
 *      Right exclusive bound of the elements to search from.
 * \param target
 *      The target value to search for.
 * \return
 *      Pointer to the first element that is greater than or equal to #target;
 *      or #end if no such element exists.
 */
template <typename T, int K = 2>
T* upperbound(T* begin, T* end, const T target)
{
    // Each iteration of the following loop reduces the search range to 1/K.
    size_t n = end - begin;
    while (n >= K) {
        end = begin + n;
        size_t part = (n + K - 1) / K;
        std::array<bool, K> less;
        for (int i = 1; i < K; i++) {
            less[i] = (begin[part * i] < target);
        }
        for (int i = 1; i < K; i++) {
            begin += (less[i] ? part : 0);
        }
        n = std::min(part, static_cast<size_t>(end - begin));
    }

    // Sequential search
    for (size_t i = 0; i < n; i++) {
        if (__glibc_unlikely(!(begin[i] < target))) {
            return &begin[i];
        }
    }
    return begin + n;
}

template <typename T, int N>
bool
splitMergeJob(const int serialMergeCutOff, MergeJob<T, N>* task,
        MergeJob<T, N> subtask[2])
{
    std::array<int, N> length;
    int totalLength = 0;
    for (uint32_t i = 0; i < N; i++) {
        length[i] = task->srcEnd[i] - task->srcBegin[i];
        totalLength += length[i];
    }

    // The task is too small; we'd better process it sequentially.
    if (totalLength < serialMergeCutOff) {
        return false;
    }

    // In order to split the task as evenly as possible, find the longest
    // input source array ...
    std::array<T*, N> splitPoints;
    uint32_t longestIdx = 0;
    for (uint32_t i = 0; i < N; i++) {
        splitPoints[i] = task->srcBegin[i] + length[i] / 2;
        if (length[i] > length[longestIdx]) {
            longestIdx = i;
        }
    }

    // ... and use its median element to split other arrays.
    T* dest1 = task->dest;
    T* dest2 = task->dest;
    for (uint32_t i = 0; i < N; i++) {
        if (i != longestIdx) {
            // TODO: investigate if K-ary search would be faster
//            splitPoints[i] = upperbound<T, 4>(
            splitPoints[i] = std::upper_bound(
                    task->srcBegin[i], task->srcEnd[i],
                    *splitPoints[longestIdx]);
        }
        dest2 += (splitPoints[i] - task->srcBegin[i]);
    }

    // Put the generated subtasks into the output queue.
    for (uint32_t i = 0; i < N; i++) {
        subtask[0].srcBegin[i] = task->srcBegin[i];
        subtask[0].srcEnd[i] = splitPoints[i];
        subtask[1].srcBegin[i] = splitPoints[i];
        subtask[1].srcEnd[i] = task->srcEnd[i];
    }
    subtask[0].dest = dest1;
    subtask[1].dest = dest2;
    return true;
}

template <typename T>
void serialMerge2Way(T* xs, T* xe, T* ys, T* ye, T* dest)
{
    while (xs < xe) {
        if (ys == ye) {
            ys = xs;
            ye = xe;
            break;
        }

        T*& min = (*xs < *ys) ? xs : ys;
        *dest = std::move(*min);
        min++;
        dest++;
    }
    std::move(ys, ye, dest);
}

template <typename T>
void serialMerge3Way(T* xs, T* xe, T* ys, T* ye, T* zs, T* ze, T* dest)
{
    while (xs < xe) {
        if (ys == ye) {
            ys = xs;
            ye = xe;
            break;
        }
        if (zs == ze) {
            zs = xs;
            ze = xe;
            break;
        }

        T*& a = (*xs < *ys) ? xs : ys;
        T*& min = (*a < *zs) ? a : zs;
        *dest = std::move(*min);
        min++;
        dest++;
    }
    serialMerge2Way(ys, ye, zs, ze, dest);
}

template <typename T>
void
serialMerge4Way(T* xs, T* xe, T* ys, T* ye, T* zs, T* ze, T* us, T* ue, T* dest)
{
    while (xs < xe) {
        if (ys == ye) {
            ys = xs;
            ye = xe;
            break;
        }
        if (zs == ze) {
            zs = xs;
            ze = xe;
            break;
        }
        if (us == ue) {
            us = xs;
            ue = xe;
            break;
        }

        T*& a = (*xs < *ys) ? xs : ys;
        T*& b = (*zs < *us) ? zs : us;
        T*& min = (*a < *b) ? a : b;
        *dest = std::move(*min);
        min++;
        dest++;
    }
    serialMerge3Way(ys, ye, zs, ze, us, ue, dest);
}

template <typename T, int N>
inline void
serialMerge(MergeJob<T, N>* task)
{
    static_assert((2 <= N) && (N <= 4), "Invalid N");
    if constexpr (N == 4)
        serialMerge4Way(task->srcBegin[0], task->srcEnd[0],
                task->srcBegin[1], task->srcEnd[1],
                task->srcBegin[2], task->srcEnd[2],
                task->srcBegin[3], task->srcEnd[3],
                task->dest);
    else if constexpr (N == 3)
        serialMerge3Way(task->srcBegin[0], task->srcEnd[0],
                task->srcBegin[1], task->srcEnd[1],
                task->srcBegin[2], task->srcEnd[2],
                task->dest);
    else if constexpr (N == 2)
        serialMerge2Way(task->srcBegin[0], task->srcEnd[0],
                task->srcBegin[1], task->srcEnd[1],
                task->dest);
}

template <typename T, int N>
void
parallelMerge(MergeJob<T, N>* task, MergeStats perCoreStats[],
        const uint16_t mergeCutOff)
{
    MergeStats* mergeStats = &perCoreStats[Arachne::core.id];
    uint64_t startTime = Cycles::rdtsc();
    MergeJob<T, N> subtask[2];
    bool success = splitMergeJob<T, N>(mergeCutOff, task, subtask);

    // The task is too small; we'd better process it sequentially.
    if (!success) {
        serialMerge(task);
        mergeStats->mergeTasks++;
        mergeStats->mergeCycles += Cycles::rdtsc() - startTime;
        return;
    }

    Arachne::ThreadId tid = Arachne::createThread(&parallelMerge<T, N>,
            &subtask[0], perCoreStats, mergeCutOff);
    mergeStats->splitTasks++;
    mergeStats->splitCycles += Cycles::rdtsc() - startTime;
    if (tid == Arachne::NullThread) {
        parallelMerge<T, N>(&subtask[0], perCoreStats, mergeCutOff);
        parallelMerge<T, N>(&subtask[1], perCoreStats, mergeCutOff);
    } else {
        parallelMerge<T, N>(&subtask[1], perCoreStats, mergeCutOff);
        Arachne::join(tid);
    }
}

// Change the following from 0 to 1 to use the slower but more direct
// implementation of parallel splitting algorithm. The faster implementation
// fuses the splitMergeJob method and checks the problem size before spawning
// threads to ensure atomic tasks are handled inline.
#if 0
template <typename T, int N>
void
parallelSplit(MergeJob<T, N>* task, std::vector<MergeJob<T, N>> outputQueues[],
        const uint16_t mergeCutOff)
{
    TimeTrace::record("mergesort: parallelSplit invoked, core %d", Arachne::core.id);
    MergeJob<T, N> subtask[2];
    bool success = splitMergeJob<T, N>(mergeCutOff, task, subtask);

    // The task is too small; we'd better process it sequentially.
    if (!success) {
        outputQueues[Arachne::core.id].push_back(*task);
        TimeTrace::record("mergesort: generated atomic task, core %d", Arachne::core.id);
        return;
    }

    Arachne::ThreadId tid = Arachne::createThread(&parallelSplit<T, N>,
            &subtask[0], outputQueues, mergeCutOff);
    TimeTrace::record("mergesort: task splitted, core %d", Arachne::core.id);
    if (tid == Arachne::NullThread) {
        parallelSplit<T, N>(&subtask[0], outputQueues, mergeCutOff);
        parallelSplit<T, N>(&subtask[1], outputQueues, mergeCutOff);
    } else {
        parallelSplit<T, N>(&subtask[1], outputQueues, mergeCutOff);
        Arachne::join(tid);
    }
}
#else
template <typename T, int N>
void
parallelSplit(MergeJob<T, N>* task, std::vector<MergeJob<T, N>> outputQueues[],
        const uint16_t mergeCutOff)
{
    // In order to split the task as evenly as possible, find the longest
    // input source array ...
    uint32_t k = 0;
    int totalLength = 0;
    std::array<int, N> length;
    for (uint32_t i = 0; i < N; i++) {
        length[i] = task->srcEnd[i] - task->srcBegin[i];
        totalLength += length[i];
        if (length[i] > length[k]) {
            k = i;
        }
    }
    if (totalLength <= mergeCutOff) {
        outputQueues[Arachne::core.id].push_back(*task);
        return;
    }

    // ... and use its median element to split other arrays.
    MergeJob<T, N> subtasks[2];
    subtasks[0].dest = task->dest;
    subtasks[1].dest = task->dest;
    std::array<T*, N> splitters;
    splitters[k] = task->srcBegin[k] + length[k] / 2;
    for (uint32_t i = 0; i < N; i++) {
        if (i != k) {
            // TODO: investigate if K-ary search would be faster
//            splitters[i] = upperbound<T, 3>(task->srcBegin[i], task->srcEnd[i],
            splitters[i] = std::upper_bound(task->srcBegin[i], task->srcEnd[i],
                    *splitters[k]);
        }
        subtasks[1].dest += (splitters[i] - task->srcBegin[i]);
    }

    // Create the subtasks and delegate the first task to another thread. Note
    // that we prefer to handle the new subtasks in the same thread unless
    // they can be further splitted.
    for (uint32_t i = 0; i < N; i++) {
        subtasks[0].srcBegin[i] = task->srcBegin[i];
        subtasks[0].srcEnd[i] = splitters[i];
        subtasks[1].srcBegin[i] = splitters[i];
        subtasks[1].srcEnd[i] = task->srcEnd[i];
    }

    Arachne::ThreadId child = Arachne::NullThread;
    bool firstTask = true;
    for (auto& subtask : subtasks) {
        int problemSize = firstTask ? (subtasks[1].dest - subtasks[0].dest) :
                (task->dest + totalLength - subtasks[1].dest);
        if (problemSize > mergeCutOff) {
            if (firstTask) {
                child = Arachne::createThread(&parallelSplit<T, N>,
                        &subtask, outputQueues, mergeCutOff);
            } else {
                parallelSplit<T, N>(&subtask, outputQueues, mergeCutOff);
            }
        } else {
            outputQueues[Arachne::core.id].push_back(subtask);
        }
        firstTask = false;
    }
    if (child != Arachne::NullThread) {
        Arachne::join(child);
    }
}
#endif

}

#endif  // MILLISORT_MERGE_H
