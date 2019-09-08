/* Copyright (c) 2011-2016 Stanford University
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

#include "Cycles.h"
#include "DispatchExec.h"

namespace RAMCloud {

/**
 * Construct a DispatchExec.
 */
DispatchExec::DispatchExec(Dispatch* dispatch)
    : Poller(dispatch, "DispatchExec")
    , requests()
    , removeIndex(0)
    , totalRemoves(0)
    , pad()
    , lock("DispatchExec:requestLock")
    , addIndex(0)
    , totalAdds(0)
{
        int result = posix_memalign(reinterpret_cast<void**>(&requests),
                CACHE_LINE_SIZE, sizeof(LambdaBox) * NUM_WORKER_REQUESTS);
        if (result != 0) {
            DIE("posix_memalign returned %s", strerror(result));
        }
        // Double checking to make sure we get proper cache alignment.
        assert((reinterpret_cast<uint64_t>(requests) & 0x3f) == 0);

        // Zero-initialize the LambdaBox array so that all the 'full' bits are
        // clear at the outset.
        memset(requests, 0, NUM_WORKER_REQUESTS * sizeof(pad));
}

/**
 * Destructor for DispatchExec
 */
DispatchExec::~DispatchExec()
{
    if (requests != NULL)
        free(requests);

    requests = NULL;
}

/**
 * Wait for a previously-scheduled piece of work to have been
 * executed.
 *
 * \param id
 *      The return value from a previous indication of addRequest:
 *      identifies the work we want to wait for.
 */
void
DispatchExec::sync(uint64_t id)
{
    // The only nontrivial thing here is that we want to print log messages
    // if the sync takes an unreasonable amount of time. Print the first
    // message after 10ms, then another message ever second after that.
    uint64_t start = Cycles::rdtsc();
    uint64_t nextLogTime = start + ((uint64_t) Cycles::perSecond())/100;
    while (totalRemoves < id) {
        uint64_t now = Cycles::rdtsc();
        if (now >= nextLogTime) {
            RAMCLOUD_LOG(WARNING, "DispatchExec::sync has been stalled for "
                    "%.2f seconds", Cycles::toSeconds(now - start));
            nextLogTime = now + (uint64_t) Cycles::perSecond();
        }
        if (owner->isDispatchThread())
            owner->poll();
    }
}

/**
 * Check if a previously-scheduled piece of work has been executed.
 *
 * \param id
 *      The return value from a previous indication of addRequest:
 *      identifies the work we want to check for completion.
 */
bool
DispatchExec::isDone(uint64_t id)
{
    // TODO: seems like a data race on totalRemoves: write in poll() from
    // dispatch but read in sync/isDone from workers
    if (totalRemoves >= id) {
        return true;
    }

    if (owner->isDispatchThread())
        owner->poll();
    return false;
}
}
