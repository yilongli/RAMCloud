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

class ClockSynchronizer : Dispatch::Poller {
  public:
    explicit ClockSynchronizer(Context* context);
    ~ClockSynchronizer() {}
    uint64_t handleRequest(ServerId callerId, uint64_t clientTsc,
            uint64_t serverTsc);
    int poll();
    void start();

  PRIVATE:
    void computeOffset();

    /// Shared RAMCloud information.
    Context* context;

    /// Store the clock offsets between this node and other nodes in the cluster
    /// when the synchronization completes.
    std::unordered_map<ServerId, int64_t> clockOffset;

    /// Time (in rdtsc ticks) the clock synchronizer is instantiated.
    const uint64_t initTime;

    /// When (in rdtsc ticks) should we update the serverTracker.
    uint64_t nextUpdateTime;

    // fixme: rename "probe"; it's now {client, server}Diff
    /// Records incoming ClockSync RPC requests.
    /// {local_server_tsc, local_server_tsc - remote_client_tsc}
//    using IncomingProbe = std::pair<uint64_t, int64_t>;
    std::unordered_map<ServerId, int64_t> incomingProbes;

    /// Records responses of outgoing ClockSync RPCs.
    std::unordered_map<ServerId, int64_t> outgoingProbes;

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
