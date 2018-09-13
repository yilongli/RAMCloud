/* Copyright (c) 2018 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "AdminService.h"
#include "ClockSynchronizer.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 1

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef. Arguments
// are made uint64_t (as opposed to uin32_t) so the caller doesn't have to
// frequently cast their 64-bit arguments into uint32_t explicitly: we will
// help perform the casting internally.
namespace {
    inline void
    timeTrace(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

ClockSynchronizer::ClockSynchronizer(Context* context)
    : Dispatch::Poller(context->dispatch, "ClockSynchronizer")
    , context(context)
    , clockOffset()
    , initTime(Cycles::rdtsc())
    , nextUpdateTime(0)
    , incomingProbes()
    , outgoingProbes()
    , outstandingRpc()
    , sendingRpc(false)
    , serverTracker(context)
    , sessions()
    , syncStopTime()
{
    assert(context->serverList);
}

void
ClockSynchronizer::computeOffset()
{
    clockOffset.clear();

    // TODO: how to iterate over all serverId using serverTrack?
    for (auto& p : outgoingProbes) {
        ServerId targetId = p.first;
        int64_t clientDiff = p.second;
        if (incomingProbes.find(targetId) == incomingProbes.end()) {
            // TODO: LOG ERROR
            continue;
        }
        int64_t serverDiff = incomingProbes[targetId];
        int64_t rtt = clientDiff + serverDiff;
        int64_t offset = (serverDiff - clientDiff) / 2;
        LOG(NOTICE, "serverDiff = %ld, clientDiff = %ld", serverDiff, clientDiff);
        LOG(NOTICE, "Clock sync'ed with server %u, RTT = %lu ns, "
                "offset = %ld cyc", targetId.indexNumber(),
                Cycles::toNanoseconds(static_cast<uint64_t>(rtt)), offset);
        clockOffset[targetId] = offset;
    }

    // FIXME: check data race; this method is run in a worker thread now
    incomingProbes.clear();
    outgoingProbes.clear();
}

uint64_t
ClockSynchronizer::handleRequest(ServerId callerId, uint64_t clientTsc,
        uint64_t serverTsc)
{
    assert(context->dispatch->isDispatchThread());
    serverTsc -= initTime;
    int64_t diff = int64_t(serverTsc) - int64_t(clientTsc);
    if (context->dispatch->currentTime <
            syncStopTime.load(std::memory_order_relaxed)) {
        auto it = incomingProbes.find(callerId);
        if (it == incomingProbes.end()) {
            incomingProbes[callerId] = diff;
        } else if (it->second > diff) {
            it->second = diff;
        }
    }
    return serverTsc;
}

int
ClockSynchronizer::poll()
{
    // Workaround to avoid reentrance due to ServerIdRpcWrapper::send()
    if (sendingRpc) {
        return 0;
    }

    // Clock synchronization only needs to be done infrequently. We don't want
    // to slow down the dispatch thread in common case.
    uint64_t currentTime = context->dispatch->currentTime;
    uint64_t stopTime = syncStopTime.load(std::memory_order_relaxed);
    if (currentTime > stopTime) {
        return 0;
    }

    // Update our serverTracker from time to time.
    if (currentTime > nextUpdateTime) {
        ServerDetails serverDetails;
        ServerChangeEvent changeEvent;
        while (serverTracker.getChange(serverDetails, changeEvent)) {}
        nextUpdateTime = currentTime + Cycles::fromMicroseconds(10000);
    }

    // To keep the system as idle as possible during clock sync., allow only
    // one outstanding RPC at a time.
    if (outstandingRpc && !outstandingRpc->isReady()) {
        return 0;
    }

    if (outstandingRpc && outstandingRpc->isReady()) {
        ClockSyncRpc* rpc = outstandingRpc.get();
        ServerId targetId = rpc->targetId;
        int64_t result = rpc->wait();
        auto it = outgoingProbes.find(targetId);
        if (it == outgoingProbes.end()) {
            outgoingProbes[targetId] = result;
        } else if (it->second > result) {
            int64_t rtt = (result + incomingProbes[targetId]);
            LOG(NOTICE, "min. tsc diff. %ld, est. RTT %.2f us, "
                    "completion time %.2f us", result,
                    Cycles::toSeconds(rtt)*1e6,
                    Cycles::toSeconds(rpc->getCompletionTime())*1e6);
            timeTrace("min. tsc diff. %d, RTT %ld, completion time %u ns",
                    int(result), rtt, Cycles::toNanoseconds(rpc->getCompletionTime()));
            it->second = result;
        }
        static int64_t baseline = result;
        timeTrace("clock sync. rpc finished, RTT %u ns, TSC diff %d + %d",
                Cycles::toNanoseconds(rpc->getCompletionTime()),
                int(baseline), int(result-baseline));
        outstandingRpc.destroy();
    }

    // Pick a random node in the cluster to probe.
    ServerId target = serverTracker.getRandomServerIdWithService(
            WireFormat::ADMIN_SERVICE);
    if (!sessions[target]) {
        sendingRpc = true;
        sessions[target] = context->serverList->getSession(target);
        sendingRpc = false;
    }
    ServerId ourServerId = context->getAdminService()->serverId;
    if (target.isValid() && target != ourServerId) {
        timeTrace("about to send clock sync. RPC");
        outstandingRpc.construct(context, sessions[target], initTime, target,
                ourServerId);
    }

    return 1;
}

void
ClockSynchronizer::start()
{
    LOG(NOTICE, "ClockSynchronizer started");
    timeTrace("ClockSynchronizer started");
    uint64_t runTime = Cycles::fromSeconds(1);
//    uint64_t runTime = Cycles::fromSeconds(.1);
    syncStopTime = Cycles::rdtsc() + runTime;
    Arachne::sleep(Cycles::toNanoseconds(runTime));
    computeOffset();
    LOG(NOTICE, "ClockSynchronizer stopped");
}

} // namespace RAMCloud
