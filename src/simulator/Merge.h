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

#include "Common.h"
#include <algorithm>

namespace RAMCloud {

template <typename T, int N = 4>
struct MergeJob {
    T* srcBegin[N];
    T* srcEnd[N];
    T* dest;

    // No initialization.
    MergeJob() {}
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
        std::vector<MergeJob<T, N>>* outputTasks)
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
    outputTasks->emplace_back();
    auto* subtask1 = &outputTasks->back();
    for (uint32_t i = 0; i < N; i++) {
        subtask1->srcBegin[i] = task->srcBegin[i];
        subtask1->srcEnd[i] = splitPoints[i];
    }
    subtask1->dest = dest1;

    outputTasks->emplace_back();
    auto* subtask2 = &outputTasks->back();
    for (uint32_t i = 0; i < N; i++) {
        subtask2->srcBegin[i] = splitPoints[i];
        subtask2->srcEnd[i] = task->srcEnd[i];
    }
    subtask2->dest = dest2;
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

}

#endif  // MILLISORT_MERGE_H
