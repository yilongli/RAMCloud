/* Copyright (c) 2014-2016 Stanford University
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

#ifndef RAMCLOUD_PERFSTATS_H
#define RAMCLOUD_PERFSTATS_H

#include <functional>
#include <unordered_map>
#include <vector>
#include "Buffer.h"
#include "SpinLock.h"

namespace RAMCloud {

/**
 * An object of this class records various performance-related information.
 * Each server thread has a private instance of this object, which eliminates
 * cash conflicts when updating statistics and makes the class thread-safe.
 * In addition, an object of this class is returned by collectStats, which
 * aggregates the statistics from all of the individual threads (this is
 * used, for example, as a result of the GET_PERF_STATS server control).
 * In order for aggregation to work, each thread must invoke the
 * registerStats method.
 *
 * If you add a new metric, be sure to update all of the relevant methods
 * in PerfStats.cc. For example, search for all of the places where
 * "collectionTime" appears and add appropriate lines for the new metric.
 *
 * This class should eventually replace RawMetrics because it is more
 * efficient (due to its use of thread-local structures).
 */
struct PerfStats {
    /// Unique identifier for this thread (threads are numbered starting
    /// at 1); Only used in thread-local instances (0 means this object
    /// contains aggregate statistics). This field isn't typically
    /// needed for gathering performance stats, but this is a convenient
    /// place to put it, since it's readily available in all threads.
    int threadId;

    /// Time (in cycles) when the statistics were gathered (only
    /// present in aggregate statistics, not in thread-local instances).
    uint64_t collectionTime;

    /// Conversion factor from collectionTime to seconds (only present
    /// in aggregate statistics, not in thread-local instances).
    double cyclesPerSecond;

    /// Total number of RAMCloud objects read (each object in a multi-read
    /// operation counts as one).
    uint64_t readCount;

    /// Total number of bytes in objects read (includes just actual object
    /// bytes; no keys or other metadata).
    uint64_t readObjectBytes;

    /// Total number of bytes in all keys for objects read (includes key
    /// metadata).
    uint64_t readKeyBytes;

    /// Total number of RAMCloud objects written (each object in a multi-write
    /// operation counts as one).
    uint64_t writeCount;

    /// Total number of bytes in objects written (includes just actual object
    /// bytes; no keys or other metadata).
    uint64_t writeObjectBytes;

    /// Total number of bytes in all keys for objects written (includes key
    /// metadata).
    uint64_t writeKeyBytes;

    /// Total time (in Cycles::rdtsc ticks) spent in calls to Dispatch::poll
    /// that did useful work (if a call to Dispatch::poll found no useful
    /// work, then it's execution time is excluded).
    uint64_t dispatchActiveCycles;

    /// Total time (in Cycles::rdtsc ticks) spent by executing RPC requests
    /// as a worker.
    uint64_t workerActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for index operations. Only one copy of PerfStats is
    // kept for all indexing structures, so the numbers below are
    // aggregates for all indexlets on a server.
    //--------------------------------------------------------------------
    /// Total number of B+ Tree nodes read for normal operations (includes
    /// nodes along the search/write paths) and split/join/re-balance operations
    uint64_t btreeNodeReads;

    /// Total number of B+ Tree nodes written (includes leaf inserts and
    /// split/join/re-balance operations)
    uint64_t btreeNodeWrites;

    /// Total number of bytes read by the B+ tree corresponding with nodeReads
    uint64_t btreeBytesRead;

    /// Total number of bytes written by the B+ tree corresponding with
    /// nodesWritten
    uint64_t btreeBytesWritten;

    /// Total number of node splits where one node becomes two (incurs at
    /// least 3 nodeWrites)
    uint64_t btreeNodeSplits;

    /// Total number of node coalesces where two nodes become one (incurs at
    /// least 2 nodeWrites)
    uint64_t btreeNodeCoalesces;

    /// Total number of B+ tree re-balances where two sibling nodes reshuffle
    /// the BtreeEntries between the two (incurs 3 node writes)
    uint64_t btreeRebalances;

    //--------------------------------------------------------------------
    // Statistics for log replication follow below. These metrics are
    // related to new information appended to the head segment (i.e., not
    // including cleaning).
    //--------------------------------------------------------------------
    /// Total bytes appended to the log head.
    uint64_t logBytesAppended;

    /// Number of replication RPCs made to the primary replica of a head
    /// segment.
    uint64_t replicationRpcs;

    /// Total time (in cycles) spent by worker threads waiting for log
    /// syncs (i.e. if 2 threads are waiting at once, this counter advances
    /// at twice real time).
    uint64_t logSyncCycles;

    /// Total time (in cycles) spent by segments in a state where they have
    /// at least one replica that has not yet been successfully opened. If
    /// this value is significant, it probably means that backups don't have
    /// enough I/O bandwidth to keep up with replication traffic, so they are
    /// rejecting open requests.
    uint64_t segmentUnopenedCycles;

    //--------------------------------------------------------------------
    // Statistics for the log cleaner follow below.
    //--------------------------------------------------------------------
    /// Total number of bytes read by the log compactor.
    uint64_t compactorInputBytes;

    /// Total bytes of live data copied to new segments by the log compactor.
    uint64_t compactorSurvivorBytes;

    /// Total time (in Cycles::rdtsc ticks) spent executing the compactor.
    uint64_t compactorActiveCycles;

    /// Total number of memory bytes that were cleaned by the combined
    /// cleaner (i.e., the combined sizes of all the in-memory segments that
    /// were freed after their contents were cleaned and live data
    /// written to new segments).
    uint64_t cleanerInputMemoryBytes;

    /// Total number of disk bytes that were cleaned by the combined cleaner
    /// (these bytes were not actually read, since the cleaner operates on
    /// in-memory information).
    uint64_t cleanerInputDiskBytes;

    /// Total bytes of live data copied to new segments by the combined
    /// cleaner.
    uint64_t cleanerSurvivorBytes;

    /// Total time (in Cycles::rdtsc ticks) spent executing the combined
    /// cleaner.
    uint64_t cleanerActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for backup I/O follow below.
    //--------------------------------------------------------------------

    /// Total number of read operations from secondary storage.
    uint64_t backupReadOps;

    /// Total bytes of data read from secondary storage.
    uint64_t backupReadBytes;

    /// Total time (in Cycles::rdtsc ticks) during which secondary
    /// storage device(s) were actively performing backup reads.
    uint64_t backupReadActiveCycles;

    /// Total bytes of data appended to backup segments.
    uint64_t backupBytesReceived;

    /// Total number of write operations from secondary storage.
    uint64_t backupWriteOps;

    /// Total bytes of data written to secondary storage. This will be
    /// more than backupBytesReceived because I/O is rounded up to even
    /// numbers of blocks (see MultiFileStorage::BLOCK_SIZE).
    uint64_t backupWriteBytes;

    /// Total time (in Cycles::rdtsc ticks) during which secondary
    /// storage device(s) were actively performing backup writes.
    uint64_t backupWriteActiveCycles;

    //--------------------------------------------------------------------
    // Statistics for the migration follow below.
    //--------------------------------------------------------------------

    /// Total number of bytes put into migration segments for transmit in
    /// Phase 1.
    uint64_t migrationPhase1Bytes;

    /// Total amount of time spent putting items into migration segments,
    /// transmitting them across the network, and waiting for the remote
    /// side to complete replay during Phase 1.
    uint64_t migrationPhase1Cycles;

    //--------------------------------------------------------------------
    // Statistics for the network follow below.
    //--------------------------------------------------------------------

    /// Total bytes received from the network via all transports.
    uint64_t networkInputBytes;

    /// Total bytes transmitted on the network by all transports.
    uint64_t networkOutputBytes;

    /// Total packets received from the network via all transports.
    uint64_t networkInputPackets;

    /// Total packets transmitted on the network by all transports.
    uint64_t networkOutputPackets;

    // TODO: use only transportActiveCycles, driverTxCycles, and driverRxCycles
    // across all transports and drivers?

    /// Total rdtsc ticks spent in calls to BasicTransport::poll that did
    /// useful work (if a call to Dispatch::poll found no useful work, then
    /// it's execution time is excluded), BasicTransport::Session:sendRequest,
    /// and BasicTransport::ServerRpc::sendReply.
    uint64_t basicTransportActiveCycles;

    /// Total rdtsc ticks spent in calls to BasicTransport::handlePacket, which
    /// implements the protocol logic including the receiver-side scheduling
    /// algorithm.
    uint64_t basicTransportHandlePacketCycles;

    /// Total time (in Cycles::rdtsc ticks) spent in receiving all packets.
    uint64_t basicTransportReceiveCycles;

    /// Total packets received from the network.
    uint64_t basicTransportInputPackets;

    /// Total bytes of data packets (including transport header) received from
    /// the network.
    uint64_t basicTransportInputDataBytes;

    /// Total time (in Cycles::rdtsc ticks) spent in sending data packets.
    uint64_t basicTransportSendDataCycles;

    /// Total time (in Cycles::rdtsc ticks) spent in sending control packets.
    uint64_t basicTransportSendControlCycles;

    /// Total control packets transmitted on the network.
    uint64_t basicTransportOutputControlPackets;

    /// Total bytes of control packets transmitted on the network.
    uint64_t basicTransportOutputControlBytes;

    /// Total bytes of data packets (including transport header) transmitted
    /// on the network.
    uint64_t basicTransportOutputDataBytes;

    /// Total data packets transmitted on the network.
    uint64_t basicTransportOutputDataPackets;

    uint64_t infudDriverTxCycles;
    uint64_t infudDriverTxPrepareCycles;
    uint64_t infudDriverTxPostSendCycles;
    uint64_t infudDriverTxPostProcessCycles;

    uint64_t infudDriverRxCycles;
    uint64_t infudDriverRxPollCqCycles;
    uint64_t infudDriverRxRefillCycles;
    uint64_t infudDriverRxProcessPacketCycles;

    // TODO?
//    uint64_t infudDriverReleaseCycles;

    //--------------------------------------------------------------------
    // Statistics for space used by log in memory and backups.
    // Note: these are NOT counter based statistics.
    //       collectStats() will not populate these values.
    //       Instead, user must call AbstractLog::getMemoryStats() manually
    //       to obtain these values.
    //--------------------------------------------------------------------

    /// Total capacity of memory reserved for log.
    uint64_t logSizeBytes;

    /// Total space used (for both live and garbage collectable data) in log
    /// (the number of seglets currently used * size of seglet)
    uint64_t logUsedBytes;

    /// Unused log space ready for write even before log cleaning
    /// (the number of free seglets * size of seglet)
    uint64_t logFreeBytes;

    /// Total log space occupied to store live data that is not currently
    /// cleanable. This number includes all space for live objects, live
    /// completion records, ongoing transactions' prepare records.
    uint64_t logLiveBytes;

    /// The largest value the logLiveBytes allowed to be. Once logLiveBytes
    /// reaches this value, no new data can be written until objects are
    /// deleted. This limit is required to ensure that there is always enough
    /// free space for the cleaner to run.
    uint64_t logMaxLiveBytes;

    /// Log space available to write new data (logMaxLiveBytes - logLiveBytes)
    uint64_t logAppendableBytes;

    /// Backup disk spaces spent for holding replicas for data of this server.
    uint64_t logUsedBytesInBackups;

    //--------------------------------------------------------------------
    // Statistics for MilliSort follow below.
    //--------------------------------------------------------------------

    // FIXME: so which of these are aggregation-based? and which are not?

    /// # nodes participate in the millisort.
    uint64_t millisortNodes;

    /// Time (in cycles) spent by the worker thread to server millisort
    /// requests. This is the end-to-end time that millisort takes.
    uint64_t millisortTime;

    /// # data tuples on this node initially.
    uint64_t millisortInitItems;

    /// # data tuples end up on this node when the sorting completes.
    uint64_t millisortFinalItems;

    /// Size of the raw keys on this node initially, in bytes.
    uint64_t millisortInitKeyBytes;

    /// Size of the keys (including metadata) that end up on this node when the
    /// sorting completes, in bytes.
    uint64_t millisortFinalPivotKeyBytes;

    /// Size of the values on this node initially, in bytes.
    uint64_t millisortInitValueBytes;

    /// Size of the values that end up on this node when the sorting completes,
    /// in bytes.
    uint64_t millisortFinalValueBytes;

    /// 1 means true; 0 means false.
    uint64_t millisortIsPivotSorter;

    /// Starting time of the local sorting stage.
    uint64_t localSortStartTime;

    /// Time (in cycles) spent by the worker thread that invokes the local sort
    /// subroutine waiting for the sorting to complete. This is the end-to-end
    /// time that the local sort takes.
    uint64_t localSortElapsedTime;

    uint64_t localSortWorkers;

    uint64_t rearrangeInitValuesStartTime;

    uint64_t rearrangeInitValuesElapsedTime;

    uint64_t rearrangeInitValuesCycles;

    uint64_t rearrangeInitValuesWorkers;

    uint64_t partitionElapsedTime;

    /// Starting time of the gather pivots stage.
    uint64_t gatherPivotsStartTime;

    /// Time (in cycles) spent by the worker thread waiting for the
    /// gather-pivots operation to complete.
    uint64_t gatherPivotsElapsedTime;

    /// Total bytes received from the network by the gather-pivots operation.
    uint64_t gatherPivotsInputBytes;

    /// Total bytes transmitted on the network by the gather-pivots operation.
    uint64_t gatherPivotsOutputBytes;

    /// CPU time (in cycles) spent by worker threads merging the pivots received
    /// from the gather-pivots operation.
    uint64_t gatherPivotsMergeCycles;

    uint64_t gatherSuperPivotsStartTime;

    /// Time (in cycles) spent by the worker thread waiting for the
    /// gather-super-pivots operation to complete.
    uint64_t gatherSuperPivotsElapsedTime;

    /// Total bytes received from the network by the gather-super-pivots operation.
    uint64_t gatherSuperPivotsInputBytes;

    /// Total bytes transmitted on the network by the gather-super-pivots operation.
    uint64_t gatherSuperPivotsOutputBytes;

    /// CPU time (in cycles) spent by worker threads merging the super-pivots
    /// received from the gather-super-pivots operation.
    uint64_t gatherSuperPivotsMergeCycles;

    uint64_t bcastPivotBucketBoundariesStartTime;

    /// Time (in cycles) spent by the worker thread waiting for the
    /// broadcast-pivot-bucket-boundaries operation to complete.
    uint64_t bcastPivotBucketBoundariesElapsedTime;

    uint64_t shufflePivotsStartTime;

    uint64_t shufflePivotsElapsedTime;

    uint64_t mergePivotsInBucketSortCycles;

    uint64_t allGatherPivotsStartTime;

    uint64_t allGatherPivotsElapsedTime;

    uint64_t allGatherPivotsMergeCycles;

    uint64_t shuffleKeysStartTime;

    uint64_t shuffleKeysElapsedTime;

    uint64_t shuffleKeysInputBytes;

    uint64_t shuffleKeysOutputBytes;

    uint64_t shuffleKeysSentRpcs;

    uint64_t shuffleKeysReceivedRpcs;

    uint64_t shuffleKeysCopyResponseCycles;

    uint64_t onlineMergeSortStartTime;

    uint64_t onlineMergeSortElapsedTime;

    uint64_t onlineMergeSortWorkers;

    uint64_t rearrangeFinalValuesStartTime;

    uint64_t rearrangeFinalValuesElapsedTime;

    uint64_t rearrangeFinalValuesCycles;

    uint64_t rearrangeFinalValuesWorkers;

    //--------------------------------------------------------------------
    // Statistics for BigQuery follow below.
    //--------------------------------------------------------------------
    uint64_t bigQueryTime;
    uint64_t bigQueryStep1ElapsedTime;
    uint64_t bigQueryStep2ElapsedTime;
    uint64_t bigQueryStep3ElapsedTime;
    uint64_t bigQueryStep4ElapsedTime;
    uint64_t bigQueryStep5ElapsedTime;
    uint64_t bigQueryStep6ElapsedTime;
    uint64_t bigQueryStep7ElapsedTime;
    uint64_t bigQueryStep8ElapsedTime;

    //--------------------------------------------------------------------
    // Temporary counters. The values below have no pre-defined use;
    // they are intended for temporary use during debugging or performance
    // analysis. Committed code in the repo should not set these counters.
    //--------------------------------------------------------------------
    uint64_t temp1;
    uint64_t temp2;
    uint64_t temp3;
    uint64_t temp4;
    uint64_t temp5;

    //--------------------------------------------------------------------
    // Miscellaneous information
    //--------------------------------------------------------------------

    /// Objects of this type hold the differences between two PerfStats
    /// readings for a collection of servers. Each entry in the
    /// unordered_map holds all of the data for one PerfStats value,
    /// such as "networkOutputBytes", and the key is the name from
    /// the structure above. Each of the values in the unordered_map holds
    /// one number for each server, sorted by increasing server id. The
    /// value is the difference between "before" and "after" readings of
    /// that metric on they are given server. Two metrics are handled
    /// specially:
    /// * No difference is computed for cyclesPerSecond; instead, the "before"
    ///   value is used verbatim.
    /// * The unordered_map will contain an additional vector named
    ///   "serverId", whose values are the indexNumbers of the ServerId
    ///   for each of the servers.
    typedef std::unordered_map<string, std::vector<double>> Diff;

    static string formatMetric(Diff* diff, const char* metric,
            const char* formatString, double scale = 1.0);
    static string formatMetricGeneric(PerfStats::Diff* diff,
            std::function<double(vector<vector<double>>&,int)> compute,
            vector<const char*> metrics, const char* formatString);
    static string formatMetricLambda(PerfStats::Diff* diff,
            std::function<double(vector<double>&)> compute,
            vector<const char*> metrics, const char* formatString);
    static string formatMetricRate(Diff* diff, const char* metric,
            const char* formatString, double scale = 1.0);
    static string formatMetricRate(Diff* diff, const char* metric,
            const char* elapsedTime, const char* formatString,
            double scale = 1.0);
    static string formatMetricRatio(Diff* diff, const char* metric1,
            const char* metric2, const char* formatString, double scale = 1.0);
    static void clusterDiff(Buffer* before, Buffer* after, int numServers,
            PerfStats::Diff* diff);
    static void collectStats(PerfStats* total);
    static string printClusterStats(Buffer* first, Buffer* second,
            int numServers = -1);
    static void registerStats(PerfStats* stats);

    /// The following thread-local variable is used to access the statistics
    /// for the current thread.
    static __thread PerfStats threadStats;

    // This is default initialized to 0
    static __thread bool initialized;

  PRIVATE:
    static void parseStats(Buffer* rawData, std::vector<PerfStats>* results);
    static void updateAggMetric(double& avg, double& min, double& max,
            double& numSamples, double value);

    /// Used in a monitor-style fashion for mutual exclusion.
    static SpinLock mutex;

    /// Keeps track of all the PerfStat structures that have been passed
    /// to registerStats (e.g. the different thread-local structures for
    /// each thread). This allows us to find all of the structures to
    /// aggregate their statistics in collectStats.
    static std::vector<PerfStats*> registeredStats;

    /// Next value to assign for the threadId member variable.  Used only
    /// by RegisterStats.
    static int nextThreadId;
};

} // end RAMCloud

#endif  // RAMCLOUD_PERFSTATS_H
