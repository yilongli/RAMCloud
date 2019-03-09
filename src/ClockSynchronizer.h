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

#ifndef RAMCLOUD_CLOCKSYNCHRONIZER_H
#define RAMCLOUD_CLOCKSYNCHRONIZER_H

#include "AdminClient.h"
#include "Dispatch.h"
#include "ServerTracker.h"
#include "Tub.h"

namespace RAMCloud {

class TimeConverter;

/**
 * TODO: document the algorithm
 */
class ClockSynchronizer : Dispatch::Poller {
  public:
    explicit ClockSynchronizer(Context* context);
    ~ClockSynchronizer() {}
    TimeConverter getConverter(ServerId serverId);
    uint64_t handleRequest(uint64_t timestamp,
            const WireFormat::ClockSync::Request* reqHdr);
    int poll();
    void run(uint32_t seconds);

  PRIVATE:
    void computeOffset();

    /// Shared RAMCloud information.
    Context* context;

    /// Time (in rdtsc ticks) when we hypothetically reset timestamp on this
    /// node. This results in much smaller timestamps during the computation of
    /// clock offsets and, thus, reduce error in the results significantly.
    /// For example, consider the relation between two clocks:
    ///         tsc_1 = skew_factor * tsc_2 + offset
    /// Within a few seconds, we can only obtain 6~7 accurate digits after the
    /// decimal of the skew factor. Therefore, the larger tsc_1 and tsc_2 are,
    /// the more error we have in the computed offset.
    uint64_t baseTsc;
    // TODO: explain how accesses to baseTsc are synchronized?

    uint64_t oldBaseTsc;

    /// # times method run() has been called.
    std::atomic<uint32_t> epoch;

    /// Information about clock on a remote node that is sufficient to convert
    /// timestamps on that node to local timestamps.
    struct ClockState {
        /// See docs of ClockSynchronizer::baseTsc.
        uint64_t baseTsc;

        // TODO: better solution?
        uint64_t newBaseTsc;

        /// See docs of ClockSynchronizer::baseTsc.
        int64_t offset;

        /// See docs of ClockSynchronizer::baseTsc.
        double skew;

        ClockState()
            : baseTsc(), offset(), skew()
        {}
    };

    /// Stores the states of clocks on all nodes in the cluster when the
    /// synchronization completes.
    std::unordered_map<ServerId, ClockState> clockState;

    /// Provides exclusive access to clock{BaseTsc, Offset, Skew}.
    SpinLock mutex;

    /// When (in rdtsc ticks) should we update the serverTracker.
    uint64_t nextUpdateTime;

    /// Each probe basically represents a ClockSync RPC (incoming or outgoing),
    /// but it only contains information needed to compute the clock skew factor
    /// and offset.
    struct Probe {
        /// See WireFormat::ClockSync::Request::clientTsc
        uint64_t clientTsc;
        /// See WireFormat::ClockSync::Response::serverTsc
        uint64_t serverTsc;

        /// Completion time (in rdtsc ticks) of the ClockSync RPC. This is used
        /// by the the RPC sender to select the fastest probe with (hopefully)
        /// the most precise timestamps.
        uint64_t completionTime;

        /// Default constructor. Constructs a null probe with "infinitely" large
        /// completion time.
        Probe()
            : clientTsc(), serverTsc(), completionTime(~0lu)
        {}

        Probe(uint64_t clientTsc, uint64_t serverTsc, uint64_t completionTime)
            : clientTsc(clientTsc)
            , serverTsc(serverTsc)
            , completionTime(completionTime)
        {}
    };

    /// Records the fastest incoming ClockSync RPCs received from each server.
    std::unordered_map<ServerId, Probe> incomingProbes;

    /// Records the fastest outgoing ClockSync RPCs sent to each server, in each
    /// phase.
    std::unordered_map<ServerId, Probe> outgoingProbes[3];

    // TODO: Huygens-related experiments
    std::unordered_map<ServerId, std::vector<Probe>> outProbes;

    /// Current phase of the protocol. -1 means the clock sync. protocol is not
    /// running; otherwise, it must be either 0, 1, or 2.
    std::atomic<int> phase;

    int probeId;

    ServerId probeTarget;

    Probe codedProbes[2];

    /// Placeholder for the outgoing ClockSync RPC.
    Tub<ClockSyncRpc> outstandingRpc;

    // FIXME: hack
    bool sendingRpc;

    /// ServerTracker used for obtaining random servers to sync. Nothing is
    /// currently stored with servers in the tracker.
    ServerTracker<void> serverTracker;

    /// Cached sessions to target servers.
    std::unordered_map<ServerId, Transport::SessionRef> sessions;

    /// # times we have run the syncrhonization protocol.
    int syncCount;

    /// When (in rdtsc ticks) shall we we stop the synchronization process.
    std::atomic<uint64_t> syncStopTime;

    friend class TimeConverter;

    DISALLOW_COPY_AND_ASSIGN(ClockSynchronizer);
};

class TimeConverter {
  public:
    explicit TimeConverter() { valid = false; }

    explicit TimeConverter(uint64_t localBaseTsc,
            ClockSynchronizer::ClockState* remoteClock)
        : localBaseTsc(localBaseTsc)
        , remoteBaseTsc(remoteClock->baseTsc)
        , offset(static_cast<double>(remoteClock->offset))
        , skew(remoteClock->skew)
        , valid(true)
    {}

    ~TimeConverter() {};

    bool isValid() { return valid; }

    uint64_t toLocalTsc(uint64_t remoteTsc);
    uint64_t toRemoteTsc(uint64_t localTsc);
    double toRemoteTime(uint64_t localTsc);

  PRIVATE:
    // TODO: properly document baseTsc, offset, and skew in TimeConverter's
    // class doc.; have ClockSync::baseTsc refer to it.

    /// See docs of ClockSynchronizer::baseTsc.
    uint64_t localBaseTsc;

    /// See docs of ClockSynchronizer::baseTsc.
    uint64_t remoteBaseTsc;

    /// See docs of ClockSynchronizer::baseTsc.
    double offset;

    /// See docs of ClockSynchronizer::baseTsc.
    double skew;

    /// True if this converter corresponds to a clock we have synchronized with.
    bool valid;
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_CLOCKSYNCHRONIZER_H
