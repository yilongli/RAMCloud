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

#define PRINT_CLOCK_SYNC_MATRIX 1

ClockSynchronizer::ClockSynchronizer(Context* context)
    : Dispatch::Poller(context->dispatch, "ClockSynchronizer")
    , context(context)
    , baseTsc(Cycles::rdtsc())
    , clockBaseTsc()
    , clockOffset()
    , clockSkew()
    , nextUpdateTime(0)
    , incomingProbes()
    , outgoingProbes()
    , phase(-1)
    , outstandingRpc()
    , sendingRpc(false)
    , serverTracker(context)
    , sessions()
    , syncStopTime()
{
    assert(context->serverList);
}

/**
 * Compute the clock skew factors and offsets between this node and other nodes
 * in the cluster. This method is called from a worker thread and must be
 * carefully synchronized with #handleRequest and #poll that both run in the
 * dispatch thread.
 */
void
ClockSynchronizer::computeOffset()
{
    {
        // Wait till the current poll(), if any, to finish. The next poll()
        // will realize that it has passed the stop time and return immediately.
        Dispatch::Lock _(context->dispatch);
        phase = -1;
    }
    timeTrace("computeOffset released dispatch lock");
    outstandingRpc.destroy();
    clockSkew.clear();
    clockOffset.clear();

    ServerId ourServerId = context->getAdminService()->serverId;
    clockBaseTsc[ourServerId] = baseTsc;
    clockOffset[ourServerId] = 0;
    clockSkew[ourServerId] = 1.0;

    for (auto& p : outgoingProbes[1]) {
        ServerId syncee = p.first;
        
        // Use the fastest probes from phase 0 and phase 2 to compute the clock
        // skew factor.
        Probe probe1 = outgoingProbes[0][syncee];
        Probe probe2 = outgoingProbes[2][syncee];
        double skew = double(probe2.clientTsc - probe1.clientTsc) /
                double(probe2.serverTsc - probe1.serverTsc);
        clockSkew[syncee] = skew;

        // Then use the fastest outgoing probe and incoming probe to compute
        // the clock offset.
        Probe incoming = incomingProbes[syncee];
        Probe outgoing = p.second;
        double localElapsed = double(incoming.serverTsc) -
                double(outgoing.clientTsc);
        double remoteElapsed = double(incoming.clientTsc) -
                double(outgoing.serverTsc);
        uint64_t halfRTT = static_cast<uint64_t>(
                (localElapsed - skew * remoteElapsed) / (1 + skew));
        int64_t offset = int64_t(outgoing.clientTsc + halfRTT) -
                static_cast<int64_t>(skew * double(outgoing.serverTsc));
        clockOffset[syncee] = offset;
        LOG(NOTICE, "Synchronized clock with server %u; est. RTT = %.2f us; "
                "TSC_%u - %lu = %.8f * (TSC_%u - %lu) + %ld",
                syncee.indexNumber(), Cycles::toSeconds(2*halfRTT)*1e6,
                ourServerId.indexNumber(), clockBaseTsc[ourServerId], skew,
                syncee.indexNumber(), clockBaseTsc[syncee], offset);
    }

#if PRINT_CLOCK_SYNC_MATRIX
    std::vector<ServerId> serverIds;
    for (auto& p : clockSkew) {
        serverIds.push_back(p.first);
    }
    std::sort(serverIds.begin(), serverIds.end());
    for (size_t i = 0; i < serverIds.size(); i++) {
        ServerId server1 = serverIds[i];
        for (size_t j = i + 1; j < serverIds.size(); j++) {
            ServerId server2 = serverIds[j];
            double skew = clockSkew[server2] / clockSkew[server1];
            int64_t offset = static_cast<int64_t>(
                    double(clockOffset[server2] - clockOffset[server1]) /
                    clockSkew[server1]);
            LOG(WARNING, "TSC_%u - %lu = %.8f * (TSC_%u - %lu) + %ld",
                    server1.indexNumber(), clockBaseTsc[server1], skew,
                    server2.indexNumber(), clockBaseTsc[server2], offset);
        }
    }
#endif

    incomingProbes.clear();
    outgoingProbes[0].clear();
    outgoingProbes[1].clear();
    outgoingProbes[2].clear();
}

/**
 * The actual RPC handler for CLOCK_SYNC request. AdminServer::clockSync simply
 * forwards the request here for processing so that we can easily access the
 * internal state of ClockSynchronizer.
 *
 * This handler is run directly in the dispatch thread for performance. This
 * also simplifies our synchronization policy in this class.
 *
 * \param timestamp
 *      Time, in Cycles::rdtsc ticks, when the handler is invoked.
 * \param reqHdr
 *      CLOCK_SYNC request header.
 * \return
 *      The logical timestamp when the handler is invoked.
 */
uint64_t
ClockSynchronizer::handleRequest(uint64_t timestamp,
        const WireFormat::ClockSync::Request* reqHdr)
{
    assert(context->dispatch->isDispatchThread());
    uint64_t serverTsc = timestamp - baseTsc;
    // We shall not modify incomingProbes once computeOffset() has started.
    if (phase.load(std::memory_order_acquire) >= 0) {
        ServerId caller(reqHdr->callerId);
        clockBaseTsc[caller] = reqHdr->baseTsc;
        incomingProbes[caller].clientTsc = reqHdr->fastestClientTsc;
        incomingProbes[caller].serverTsc = reqHdr->fastestServerTsc;
    }
    return serverTsc;
}

/**
 * Send ClockSync RPCs to random nodes in the cluster back-to-back.
 *
 * \return
 *      0, if no useful work has been performed; 1, otherwise.
 */
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
        int index = phase.load(std::memory_order_relaxed);
        Probe* fastestProbe = &outgoingProbes[index][targetId];
        if (rpc->getCompletionTime() < fastestProbe->completionTime) {
            fastestProbe->clientTsc = rpc->getClientTsc();
            fastestProbe->serverTsc = rpc->wait();
            fastestProbe->completionTime = rpc->getCompletionTime();
            timeTrace("update fastest probe, completion time %u ns",
                    Cycles::toNanoseconds(rpc->getCompletionTime()));
        }
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
        Probe* fastestProbe = &outgoingProbes[1][target];
        outstandingRpc.construct(context, sessions[target], baseTsc, target,
                ourServerId, fastestProbe->clientTsc, fastestProbe->serverTsc);
    }

    return 1;
}

/**
 * Run the clock synchronization protocol for a given period of time.
 * Return after the clock synchronization process completes.
 *
 * \param seconds
 *      Time to run, in seconds.
 */
void
ClockSynchronizer::run(uint32_t seconds)
{
    LOG(NOTICE, "Start running clock sync. for %u seconds", seconds);
    timeTrace("ClockSynchronizer started");
    uint64_t totalTimeNs = seconds * 1000000000UL;
    uint64_t timeNs[3];
    timeNs[0] = totalTimeNs / 10;
    timeNs[2] = totalTimeNs / 10;
    timeNs[1] = totalTimeNs - timeNs[0] - timeNs[2];
    uint64_t wakeupTime = Cycles::rdtsc() + Cycles::fromSeconds(seconds);
    phase = 0;
    syncStopTime = wakeupTime;
    Arachne::sleep(timeNs[0]);
    phase++;
    Arachne::sleep(timeNs[1]);
    phase++;
    Arachne::sleep(timeNs[2]);
    while (true) {
        uint64_t now = Cycles::rdtsc();
        // The following can happen because our Cycles::perSecond() is different
        // from Arachne's Cycles::perSecond().
        if (now < wakeupTime) {
            Arachne::sleep(Cycles::toNanoseconds(wakeupTime - now));
        } else {
            break;
        }
    }
    computeOffset();
    LOG(NOTICE, "ClockSynchronizer stopped");
}

} // namespace RAMCloud
