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

/**
 * TODO: document the algorithm
 */
class ClockSynchronizer : Dispatch::Poller {
  public:
    explicit ClockSynchronizer(Context* context);
    ~ClockSynchronizer() {}
    uint64_t handleRequest(uint64_t timestamp,
            const WireFormat::ClockSync::Request* reqHdr);
    int poll();
    void run(uint32_t seconds);
    uint64_t toLocalTsc(ServerId remote, uint64_t remoteTsc);
    uint64_t toRemoteTsc(ServerId remote, uint64_t localTsc);

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
    const uint64_t baseTsc;

    // FIXME: introduce struct ClockState = {baseTsc, offset, skew}; combine 3 map accesses into one!
    /// Stores the clock base times of all nodes in the cluster when the
    /// synchronization completes.
    std::unordered_map<ServerId, uint64_t> clockBaseTsc;

    /// Stores the clock offsets between this node and other nodes in the cluster
    /// when the synchronization completes.
    std::unordered_map<ServerId, int64_t> clockOffset;

    /// Stores the clock skew factor between this node and other nodes in the
    /// cluster when the synchronization completes.
    std::unordered_map<ServerId, double> clockSkew;

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
    };

    /// Records the fastest incoming ClockSync RPCs received from each server.
    std::unordered_map<ServerId, Probe> incomingProbes;

    /// Records the fastest outgoing ClockSync RPCs sent to each server, in each
    /// phase.
    std::unordered_map<ServerId, Probe> outgoingProbes[3];

    /// Current phase of the protocol. -1 means the clock sync. protocol is not
    /// running; otherwise, it must be either 0, 1, or 2.
    std::atomic<int> phase;

    /// Placeholder for the outgoing ClockSync RPC.
    Tub<ClockSyncRpc> outstandingRpc;

    // FIXME: hack
    bool sendingRpc;

    /// ServerTracker used for obtaining random servers to sync. Nothing is
    /// currently stored with servers in the tracker.
    ServerTracker<void> serverTracker;

    /// Cached sessions to target servers.
    std::unordered_map<ServerId, Transport::SessionRef> sessions;

    /// When (in rdtsc ticks) shall we we stop the synchronization process.
    std::atomic<uint64_t> syncStopTime;

    DISALLOW_COPY_AND_ASSIGN(ClockSynchronizer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_CLOCKSYNCHRONIZER_H
