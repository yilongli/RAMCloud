/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_UTIL_H
#define RAMCLOUD_UTIL_H

#include <sys/syscall.h>
#include <time.h>
#include "Common.h"

namespace RAMCloud {

/**
 * Miscellaneous methods that seem like they might be useful in several
 * different places.
 */
namespace Util {

void clearCpuAffinity(void);
int takeOneCore(void);
void genRandomString(char* str, const int length);
string getCpuAffinityString(void);
vector<int> getAffinedCpus(void);
string hexDump(const void *buffer, uint64_t bytes);
void setThreadPriorityHigh();
void spinAndCheckGaps(int count);
bool timespecLess(const struct timespec& t1, const struct timespec& t2);
bool timespecLessEqual(const struct timespec& t1, const struct timespec& t2);
struct timespec timespecAdd(const struct timespec& t1,
        const struct timespec& t2);
int getHyperTwin(int coreId);
void arachnePinCoreManually();

extern uint64_t mockPmcValue;

/* Doxygen is stupid and cannot distinguish between attributes and arguments. */
#define FORCE_INLINE __inline __attribute__((always_inline))

/**
 * Returns the thread id of the calling thread
 * As long as the thread continues to run, this id is unique across all threads
 * running on the system so it can be used to uniquely name per-thread
 * resources
 */
static
int FORCE_INLINE
gettid() {
    return static_cast<int>(syscall(SYS_gettid));
}

/**
 * A utility for function for calling rdpmc and reading Intel's performance
 * counters. Returns the value of the performance monitoring counter with
 * index specified by argumemt ecx.
 *
 * \param ecx
 *    The index of the PMC register to read the value from. The correct
 *    value of this parameter is dependent on the selected pmc.
 *
 * NB:
 *
 * 1. This function will segfault if called in userspace unless the 8th bit
 *    of the CR4 register is set.
 * 2. This function's behavior will change depending on which pmc's have been
 *    selected. The selection is done using wrmsr from inside a kernel module.
 */
static
uint64_t FORCE_INLINE
readPmc(int ecx)
{
#ifdef TESTING
    if (mockPmcValue)
        return mockPmcValue;
#endif
    unsigned int a, d;
    __asm __volatile("rdpmc" : "=a"(a), "=d"(d) : "c"(ecx));
    return ((uint64_t)a) | (((uint64_t)d) << 32);
}

/**
 * This function pins the currently executing thread onto the CPU Core with
 * the id given in the argument.
 *
 * \param id
 *      The id of the core to pin the caller's thread to.
 */
static FORCE_INLINE
void pinThreadToCore(int id) {
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(id, &cpuset);
    int r = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    if (r != 0) {
        RAMCLOUD_DIE("sched_setaffinity failed: %s, id %d, affinity mask %s",
                strerror(errno), id, getCpuAffinityString().c_str());
    }
}

/**
 * Returns the cpu affinity mask of the currently executing thread. The type
 * cpu_set_t encodes information about which cores the current thread is
 * permitted to run on.
 */
static FORCE_INLINE
cpu_set_t getCpuAffinity() {
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    int r = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    if (r != 0) {
        RAMCLOUD_DIE("sched_getaffinity failed: %s", strerror(errno));
    }
    return cpuset;
}

/**
 * This function sets the allowable set of cores for the currently executing
 * thread, and is usually used to restore an older set which was read using
 * getCpuAffinity().
 *
 * \param cpuset
 *      An object of type cpu_set_t which encodes the set of cores which
 *      current thread is permitted to run on.
 */
static FORCE_INLINE
void setCpuAffinity(cpu_set_t cpuset) {
    int r = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    if (r != 0) {
        RAMCLOUD_DIE("sched_setaffinity failed: %s", strerror(errno));
    }
}

/**
 * This function is used to seralize machine instructions so that no
 * instructions that appear after it in the current thread can run before any
 * instructions that appear before it.
 *
 * It is useful for putting around rdpmc instructions (to pinpoint cache
 * misses) as well as before rdtsc instructions, to prevent time pollution from
 * instructions supposed to be executing before the timer starts.
 */
static FORCE_INLINE
void serialize() {
    uint32_t eax, ebx, ecx, edx;
    __asm volatile("cpuid"
        : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
        : "a" (1U));
}

/**
 * This is a convenience function to make a call to rdpmc with serializing
 * wrappers to ensure all earlier instructions have executed and no later
 * instructions have executed.
 *
 * \param ecx
 *      The index of the PMC register to read the value from. The correct
 *      value of this parameter is dependent on the selected pmc.
 */
static FORCE_INLINE
uint64_t
serialReadPmc(int ecx)
{
    serialize();
    uint64_t retVal = readPmc(ecx);
    serialize();
    return retVal;
}

static FORCE_INLINE
uint64_t
wyhash64() {
    // State for wyhash64; can be seeded with any value.
    static thread_local uint64_t wyhash64_x = generateRandom();

    wyhash64_x += UINT64_C(0x60bee2bee120fc15);
    __uint128_t tmp;
    tmp = (__uint128_t)wyhash64_x * UINT64_C(0xa3b195354a39b70d);
    uint64_t m1 = (tmp >> 64) ^ tmp;
    tmp = (__uint128_t)m1 * UINT64_C(0x1b03738712fad5c9);
    uint64_t m2 = (tmp >> 64) ^ tmp;
    return m2;
}

static FORCE_INLINE
uint32_t
wyhash32() {
    return wyhash64();
}

} // end Util

} // end RAMCloud

#undef FORCE_INLINE
#endif  // RAMCLOUD_UTIL_H
