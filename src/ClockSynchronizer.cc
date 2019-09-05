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

#include <cmath>
#include "AdminService.h"
#include "ClockSynchronizer.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

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

#define PRINT_CLOCK_SYNC_MATRIX 0

#define HUYGENS_EXPERIMENT 0

ClockSynchronizer::ClockSynchronizer(Context* context)
    : Dispatch::Poller(context->dispatch, "ClockSynchronizer")
    , context(context)
    , baseTsc()
    , oldBaseTsc()
//    , baseTsc(Cycles::rdtsc())
    , epoch()
    , clockState()
    , mutex("ClockSynchronizer::clockState")
    , nextUpdateTime(0)
    , incomingProbes()
    , outgoingProbes()
    , outProbes()
    , phase(-1)
    , probeId(0)
    , probeTarget()
    , codedProbes()
    , outstandingRpc()
    , sendingRpc(false)
    , serverTracker(context)
    , sessions()
    , syncCount(0)
    , syncStopTime()
{
    assert(context->serverList);
    context->clockSynchronizer = this;
}

/**
 * Compute the clock skew factors and offsets between this node and other nodes
 * in the cluster. This method is run inside the dispatch thread (so does #poll
 * and #handleRequest), via the DispatchExec mechanism, to simplify the
 * synchronization.
 */
void
ClockSynchronizer::computeOffset()
{
    if (!context->dispatch->isDispatchThread()) {
        DIE("ClockSynchronizer::computeOffset must run in the dispatch thread");
    }

    LOG(NOTICE, "ClockSynchronizer::ComputeOffsetWrapper: start to compute "
            "clock skew factors and offsets");

    phase = -1;
    syncCount++;

    SpinLock::Guard _(mutex);
    outstandingRpc.destroy();

    ServerId ourServerId = context->getAdminService()->serverId;
    clockState[ourServerId].baseTsc = baseTsc;
    clockState[ourServerId].skew = 1.0;

    uint64_t now = Cycles::rdtsc();
    for (auto& p : outgoingProbes[1]) {
        ServerId syncee = p.first;
        ClockState* clock = &clockState[syncee];
        double oldRemoteTime = 0;
        double newRemoteTime;
        if (oldBaseTsc > 0) {
            oldRemoteTime = TimeConverter(oldBaseTsc, clock).toRemoteTime(now);
        }
        clock->baseTsc = clock->newBaseTsc;

        // Use the fastest probes from phase 0 and phase 2 to compute the clock
        // skew factor.
        Probe probe1 = outgoingProbes[0][syncee];
        Probe probe2 = outgoingProbes[2][syncee];
        double skew = double(probe2.clientTsc - probe1.clientTsc) /
                double(probe2.serverTsc - probe1.serverTsc);
        clock->skew = skew;
        if (probe1.invalid() || probe2.invalid()) {
            DIE("Failed to sync. clock with server %u: no qualified "
                    "clean probe found in phase %u", syncee.indexNumber(),
                    probe1.invalid() ? 0 : 2);
        }

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
        double roundTripMicros = Cycles::toSeconds(2*halfRTT)*1e6;
        if (roundTripMicros > 5.0) {
            LOG(ERROR, "Suspiciously large RTT %.2f, localElapsed %.2f cyc, "
                    "remoteElapsed %.2f cyc, skew %.10f, probe 1 %.2f us, "
                    "probe 2 %.2f us", roundTripMicros, localElapsed,
                    remoteElapsed, skew,
                    Cycles::toSeconds(probe1.completionTime)*1e6,
                    Cycles::toSeconds(probe2.completionTime)*1e6);
        }
        int64_t offset = int64_t(outgoing.clientTsc + halfRTT) -
                static_cast<int64_t>(skew * double(outgoing.serverTsc));
        clock->offset = offset;
        newRemoteTime = TimeConverter(baseTsc, clock).toRemoteTime(now);
        if (oldRemoteTime < 1e-3) {
            oldRemoteTime = newRemoteTime;
        }
        LOG(NOTICE, "Synchronized clock with server %u round %d; RTT %.2f us; "
                "time jump %.0f ns; TSC_%u - %lu = %.8f * (TSC_%u - %lu) + %ld",
                syncee.indexNumber(), syncCount, roundTripMicros,
                newRemoteTime - oldRemoteTime, ourServerId.indexNumber(),
                clockState[ourServerId].baseTsc, skew, syncee.indexNumber(),
                clock->baseTsc, offset);
    }

#if PRINT_CLOCK_SYNC_MATRIX
    std::vector<ServerId> serverIds;
    for (auto& p : clockState) {
        serverIds.push_back(p.first);
    }
    std::sort(serverIds.begin(), serverIds.end());
    for (size_t i = 0; i < serverIds.size(); i++) {
        ServerId server1 = serverIds[i];
        ClockState* clock1 = &clockState[server1];
        for (size_t j = i + 1; j < serverIds.size(); j++) {
            ServerId server2 = serverIds[j];
            ClockState* clock2 = &clockState[server2];
            double skew = clock2->skew / clock1->skew;
            int64_t offset = static_cast<int64_t>(
                    double(clock2->offset - clock1->offset) / clock1->skew);
            LOG(NOTICE, "TSC_%u - %lu = %.8f * (TSC_%u - %lu) + %ld",
                    server1.indexNumber(), clock1->baseTsc, skew,
                    server2.indexNumber(), clock2->baseTsc, offset);
        }
    }
#endif

#if HUYGENS_EXPERIMENT
    // FIXME: Huygens-related experiment
    for (auto& p : outProbes) {
        ServerId id = p.first;

        // Compute the 5% percentile smallest completion time.
        std::vector<uint64_t> vec;
        for (auto& probe : p.second) {
            vec.push_back(probe.completionTime);
        }
        std::sort(vec.begin(), vec.end());
        int numGoodProbes = int(double(vec.size()) * 0.05) + 1;
        uint64_t threshold = vec[numGoodProbes];

        // And use that as a filter to remove noisy probes.
        string result;
        result.append(std::to_string(id.indexNumber()));
        result.append("\n");
        result.append(std::to_string(numGoodProbes));
        result.append("\n");
        for (int i = 0; i < 3; i++) {
            Probe fastest = outgoingProbes[i][id];
            result.append(std::to_string(fastest.clientTsc) + " " +
                    std::to_string(fastest.serverTsc));
            result.append("\n");
        }
        for (auto& probe : p.second) {
            if (probe.completionTime > threshold) {
                continue;
            }
            result.append(std::to_string(probe.clientTsc));
            result.append(" ");
            result.append(std::to_string(probe.serverTsc));
            result.append(" ");
            result.append(std::to_string(probe.completionTime));
            result.append("\n");
        }
        printf("%s", result.c_str());
        fflush(stdout);
    }
#endif
    outProbes.clear();

    incomingProbes.clear();
    outgoingProbes[0].clear();
    outgoingProbes[1].clear();
    outgoingProbes[2].clear();
}

TimeConverter
ClockSynchronizer::getConverter(ServerId serverId)
{
    SpinLock::Guard _(mutex);
    auto it = clockState.find(serverId);
    if (it == clockState.end()) {
        RAMCLOUD_CLOG(ERROR, "Clock not synchronized with server %s",
                serverId.toString().c_str());
        return TimeConverter();
    }
    return TimeConverter(baseTsc, &it->second);
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
    if (reqHdr->epoch > epoch.load(std::memory_order_acquire)) {
        // Client's epoch ahead of us; ignore this probe.
        return 0;
    }

    uint64_t serverTsc = timestamp - baseTsc;
    // We shall not modify incomingProbes once computeOffset() has started.
    if (phase.load(std::memory_order_acquire) >= 0) {
        ServerId caller(reqHdr->callerId);
        SpinLock::Guard _(mutex);
        if (clockState[caller].newBaseTsc != reqHdr->baseTsc) {
            clockState[caller].newBaseTsc = reqHdr->baseTsc;
        }
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
    // FIXME: a better solution is dynamically register the ClockSync'er when
    // #run is invoked and deregister it when it's done!
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
        uint64_t completionTime = ~0lu;
        uint64_t serverTsc = 0;
        try {
            serverTsc = rpc->wait();
            completionTime = rpc->getCompletionTime();
        } catch (const std::exception& e) {
            LOG(ERROR, "ClockSync RPC to server %s failed: %s",
                    targetId.toString().c_str(), e.what());
        }

        codedProbes[probeId % 2] = {rpc->getClientTsc(), serverTsc,
                completionTime};
        if (probeId % 2) {
            // Test probe pair's purity
            uint64_t clientDelta =
                    codedProbes[1].clientTsc - codedProbes[0].clientTsc;
            uint64_t serverDelta =
                    codedProbes[1].serverTsc - codedProbes[0].serverTsc;
            uint64_t error = serverDelta > clientDelta ?
                    serverDelta - clientDelta : clientDelta - serverDelta;
            if ((codedProbes[0].serverTsc == 0) ||
                    (codedProbes[1].serverTsc == 0)) {
                error = 99999;
            }
            if ((codedProbes[0].completionTime > 99999999) ||
                    (codedProbes[1].completionTime > 99999999)) {
                error = 99999;
            }
            if (error < 50) {
                int index = phase.load(std::memory_order_relaxed);
                Probe* fastestProbe = &outgoingProbes[index][targetId];
                if (completionTime < fastestProbe->completionTime) {
                    fastestProbe->clientTsc = rpc->getClientTsc();
                    fastestProbe->serverTsc = serverTsc;
                    fastestProbe->completionTime = completionTime;
                    timeTrace("update fastest probe, completion time %u ns",
                            Cycles::toNanoseconds(completionTime));
                }

                // FIXME: Huygens-related experiment
                outProbes[targetId].emplace_back(rpc->getClientTsc(), serverTsc,
                        rpc->getCompletionTime());
            }
        }
        outstandingRpc.destroy();
        probeId++;
    }

    // Pick a random node in the cluster to probe.
    if (probeId % 2 == 0) {
        probeTarget = serverTracker.getRandomServerIdWithService(
                WireFormat::ADMIN_SERVICE);
    }
    if (!sessions[probeTarget]) {
        sendingRpc = true;
        sessions[probeTarget] = context->serverList->getSession(probeTarget);
        sendingRpc = false;
    }
    ServerId ourServerId = context->getAdminService()->serverId;
    if (probeTarget.isValid() && probeTarget != ourServerId) {
        Probe* fastestProbe = &outgoingProbes[1][probeTarget];
        outstandingRpc.construct(context, sessions[probeTarget], epoch, baseTsc,
                probeTarget, ourServerId, fastestProbe->clientTsc,
                fastestProbe->serverTsc);
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
    if (phase >= 0) {
        LOG(WARNING, "Clock sync. protocol in progress; duplicate request?");
        while (phase >= 0) {
            Arachne::sleep(100 * 1000);
        }
        return;
    }

    LOG(NOTICE, "Start running clock sync. for %u seconds", seconds);
    timeTrace("ClockSynchronizer started");
    uint64_t totalTimeNs = seconds * 1000000000UL;
    uint64_t timeNs[3];
    timeNs[0] = totalTimeNs / 10;
    timeNs[2] = totalTimeNs / 10;
    timeNs[1] = totalTimeNs - timeNs[0] - timeNs[2];
    uint64_t now = Cycles::rdtsc();
    uint64_t wakeupTime = now + Cycles::fromSeconds(seconds);

    // Update baseTsc and then advance to the new epoch (the order is important)
    oldBaseTsc = baseTsc;
    baseTsc = now;
    epoch.fetch_add(1, std::memory_order_release);
    phase = 0;
    syncStopTime = wakeupTime;
    Arachne::sleep(timeNs[0]);
    phase++;
    Arachne::sleep(timeNs[1]);
    phase++;
    Arachne::sleep(timeNs[2]);
    while (true) {
        now = Cycles::rdtsc();
        // The following can happen because our Cycles::perSecond() is different
        // from Arachne's Cycles::perSecond().
        if (now < wakeupTime) {
            Arachne::sleep(Cycles::toNanoseconds(wakeupTime - now));
        } else {
            break;
        }
    }

    // Pass the final task of computing clock skew/offset to the dispatch thread
    // and wait until it's done.
    uint64_t id = context->dispatchExec->addRequest<ComputeOffsetWrapper>(this);
    while (!context->dispatchExec->isDone(id)) {
        Arachne::sleep(100 * 1000);
    }

    LOG(NOTICE, "ClockSynchronizer stopped");
}

/**
 * Convert the Cycles::rdtsc timestamp on the remote server to that on this
 * server.
 *
 * \param remoteTsc
 *      Remote rdtsc timestamp.
 * \return
 *      Local rdtsc timestamp.
 */
uint64_t
TimeConverter::toLocalTsc(uint64_t remoteTsc)
{
    // TSC_local - base_local = skew * (TSC_remote - base_remote) + offset
    return static_cast<uint64_t>(
            double(remoteTsc - remoteBaseTsc) * skew + offset) + localBaseTsc;
}

/**
 * Convert the Cycles::rdtsc timestamp on this server to that on the remote
 * server.
 *
 * \param localTsc
 *      Local rdtsc timestamp.
 * \return
 *      Remote rdtsc timestamp.
 */
uint64_t
TimeConverter::toRemoteTsc(uint64_t localTsc)
{
    // TSC_local - base_local = skew * (TSC_remote - base_remote) + offset
    return static_cast<uint64_t>(
            (double(localTsc - localBaseTsc) - offset) / skew) + remoteBaseTsc;
}

/**
 * Convert the Cycles::rdtsc timestamp on this server to POSIX time on the
 * remote server.
 *
 * \param localTsc
 *      Local rdtsc timestamp.
 * \return
 *      Remote time, in nanoseconds.
 */
double
TimeConverter::toRemoteTime(uint64_t localTsc)
{
    uint64_t remoteTsc = toRemoteTsc(localTsc);

    // Here is the tricky part. As of 09/2018, Cycles::perSecond() estimated
    // on rc machines somehow have only 4 significant digits (i.e., only the
    // integral part of cyclesPerMicros is accurate). However, remoteTsc
    // computed above is a very large number. Therefore, if we simply use
    // "Cycles::perSecond() / skew" to compute remoteCyclesPerSec, we will get
    // very different results in nanoseconds on different machines. Fortunately,
    // since we just want to align time traces and don't care much about the
    // absolute time in ns, we can live with inaccurate cyclesPerSec, we just
    // need to make sure all machines use the same value.
    uint64_t remoteCyclesPerMicros =
            static_cast<uint64_t>(Cycles::perSecond() / skew * 1e-6);
    double remoteCyclesPerSec = double(remoteCyclesPerMicros) * 1e6;
    return Cycles::toSeconds(remoteTsc, remoteCyclesPerSec) * 1e9;
}

} // namespace RAMCloud
