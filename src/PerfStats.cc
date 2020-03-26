/* Copyright (c) 2014-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <numeric>
#include <limits>
#include "Cycles.h"
#include "Minimal.h"
#include "PerfStats.h"
#include "ServerId.h"
#include "WireFormat.h"

namespace RAMCloud {

SpinLock PerfStats::mutex("PerfStats");
std::vector<PerfStats*> PerfStats::registeredStats;
int PerfStats::nextThreadId = 1;
__thread PerfStats PerfStats::threadStats;
__thread bool PerfStats::initialized;

const double DOUBLE_MAX = std::numeric_limits<double>::max();

/**
 * This method must be called to make a PerfStats structure "known" so that
 * its contents will be considered by collectStats. Typically this method
 * is invoked once for the thread-local structure associated with each
 * thread. This method is idempotent and thread-safe, so it is safe to
 * invoke it multiple times for the same PerfStats.
 *
 * \param stats
 *      PerfStats structure to remember for usage by collectStats. If this
 *      is the first time this structure has been registered, all of its
 *      counters will be initialized.
 */
void
PerfStats::registerStats(PerfStats* stats)
{
    std::lock_guard<SpinLock> lock(mutex);

    // First see if this structure is already registered; if so,
    // there is nothing for us to do.
    foreach (PerfStats* registered, registeredStats) {
        if (registered == stats) {
            return;
        }
    }

    // This is a new structure; add it to our list, and reset its contents.
    memset(stats, 0, sizeof(*stats));
    stats->threadId = nextThreadId;
    nextThreadId++;
    registeredStats.push_back(stats);
}

/**
 * This method aggregates performance information from all of the
 * PerfStats structures that have been registered via the registerStats
 * method.
 *
 * Note: this function doesn't calculate or fill memory statistics.
 *       See definition of memory stat fields (eg. logMaxBytes) for details.
 *
 * \param[out] total
 *      Filled in with the sum of all statistics from all registered
 *      PerfStat structures; any existing contents are overwritten.
 */
void
PerfStats::collectStats(PerfStats* total)
{
    std::lock_guard<SpinLock> lock(mutex);
    memset(total, 0, sizeof(*total));
    total->collectionTime = Cycles::rdtsc();
    total->cyclesPerSecond = Cycles::perSecond();
    foreach (PerfStats* stats, registeredStats) {
        // Note: the order of the statements below should match the
        // declaration order in PerfStats.h.
        total->readCount += stats->readCount;
        total->readObjectBytes += stats->readObjectBytes;
        total->readKeyBytes += stats->readKeyBytes;
        total->writeCount += stats->writeCount;
        total->writeObjectBytes += stats->writeObjectBytes;
        total->writeKeyBytes += stats->writeKeyBytes;
        total->dispatchActiveCycles += stats->dispatchActiveCycles;
        total->logBytesAppended += stats->logBytesAppended;
        total->replicationRpcs += stats->replicationRpcs;
        total->logSyncCycles += stats->logSyncCycles;
        total->segmentUnopenedCycles += stats->segmentUnopenedCycles;
        total->workerActiveCycles += stats->workerActiveCycles;
        total->btreeNodeReads += stats->btreeNodeReads;
        total->btreeNodeWrites += stats->btreeNodeWrites;
        total->btreeBytesRead += stats->btreeBytesRead;
        total->btreeBytesWritten += stats->btreeBytesWritten;
        total->btreeNodeSplits += stats->btreeNodeSplits;
        total->btreeNodeCoalesces += stats->btreeNodeCoalesces;
        total->btreeRebalances += stats->btreeRebalances;
        total->compactorInputBytes += stats->compactorInputBytes;
        total->compactorSurvivorBytes += stats->compactorSurvivorBytes;
        total->compactorActiveCycles += stats->compactorActiveCycles;
        total->cleanerInputMemoryBytes += stats->cleanerInputMemoryBytes;
        total->cleanerInputDiskBytes += stats->cleanerInputDiskBytes;
        total->cleanerSurvivorBytes += stats->cleanerSurvivorBytes;
        total->cleanerActiveCycles += stats->cleanerActiveCycles;
        total->backupReadOps += stats->backupReadOps;
        total->backupReadBytes += stats->backupReadBytes;
        total->backupReadActiveCycles += stats->backupReadActiveCycles;
        total->backupBytesReceived += stats->backupBytesReceived;
        total->backupWriteOps += stats->backupWriteOps;
        total->backupWriteBytes += stats->backupWriteBytes;
        total->backupWriteActiveCycles += stats->backupWriteActiveCycles;
        total->migrationPhase1Bytes += stats->migrationPhase1Bytes;
        total->migrationPhase1Cycles += stats->migrationPhase1Cycles;
        total->networkInputBytes += stats->networkInputBytes;
        total->networkOutputBytes += stats->networkOutputBytes;
        total->networkInputPackets += stats->networkInputPackets;
        total->networkOutputPackets += stats->networkOutputPackets;
#define COLLECT(x) total->x += stats->x
        COLLECT(basicTransportActiveCycles);
        COLLECT(basicTransportHandlePacketCycles);
        COLLECT(basicTransportReceiveCycles);
        COLLECT(basicTransportInputPackets);
        COLLECT(basicTransportInputDataBytes);
        COLLECT(basicTransportSendDataCycles);
        COLLECT(basicTransportSendControlCycles);
        COLLECT(basicTransportOutputControlPackets);
        COLLECT(basicTransportOutputControlBytes);
        COLLECT(basicTransportOutputDataBytes);
        COLLECT(basicTransportOutputDataPackets);
        COLLECT(infudDriverTxCycles);
        COLLECT(infudDriverTxPrepareCycles);
        COLLECT(infudDriverTxPostSendCycles);
        COLLECT(infudDriverTxPostProcessCycles);
        COLLECT(infudDriverRxCycles);
        COLLECT(infudDriverRxPollCqCycles);
        COLLECT(infudDriverRxRefillCycles);
        COLLECT(infudDriverRxProcessPacketCycles);
        COLLECT(millisortNodes);
        COLLECT(millisortTime);
        COLLECT(millisortInitItems);
        COLLECT(millisortFinalItems);
        COLLECT(millisortInitKeyBytes);
        COLLECT(millisortFinalPivotKeyBytes);
        COLLECT(millisortInitValueBytes);
        COLLECT(millisortFinalValueBytes);
        COLLECT(millisortIsPivotSorter);
        COLLECT(localSortStartTime);
        COLLECT(localSortElapsedTime);
        COLLECT(localSortWorkers);
        COLLECT(rearrangeInitValuesStartTime);
        COLLECT(rearrangeInitValuesElapsedTime);
        COLLECT(rearrangeInitValuesCycles);
        COLLECT(rearrangeInitValuesWorkers);
        COLLECT(partitionElapsedTime);
        COLLECT(gatherPivotsStartTime);
        COLLECT(gatherPivotsElapsedTime);
        COLLECT(gatherPivotsInputBytes);
        COLLECT(gatherPivotsOutputBytes);
        COLLECT(gatherPivotsMergeCycles);
        COLLECT(gatherSuperPivotsStartTime);
        COLLECT(gatherSuperPivotsElapsedTime);
        COLLECT(gatherSuperPivotsInputBytes);
        COLLECT(gatherSuperPivotsOutputBytes);
        COLLECT(gatherSuperPivotsMergeCycles);
        COLLECT(bcastPivotBucketBoundariesStartTime);
        COLLECT(bcastPivotBucketBoundariesElapsedTime);
        COLLECT(shufflePivotsStartTime);
        COLLECT(shufflePivotsElapsedTime);
        COLLECT(mergePivotsInBucketSortCycles);
        COLLECT(allGatherPivotsStartTime);
        COLLECT(allGatherPivotsElapsedTime);
        COLLECT(allGatherPivotsMergeCycles);
        COLLECT(shuffleKeysStartTime);
        COLLECT(shuffleKeysElapsedTime);
        COLLECT(shuffleKeysInputBytes);
        COLLECT(shuffleKeysOutputBytes);
        COLLECT(shuffleKeysReceivedRpcs);
        COLLECT(shuffleKeysSentRpcs);
        COLLECT(shuffleKeysCopyResponseCycles);
        COLLECT(onlineMergeSortStartTime);
        COLLECT(onlineMergeSortElapsedTime);
        COLLECT(onlineMergeSortWorkers);
        COLLECT(rearrangeFinalValuesStartTime);
        COLLECT(rearrangeFinalValuesElapsedTime);
        COLLECT(rearrangeFinalValuesCycles);
        COLLECT(rearrangeFinalValuesWorkers);
        COLLECT(bigQueryTime);
        COLLECT(bigQueryStep1ElapsedTime);
        COLLECT(bigQueryStep2ElapsedTime);
        COLLECT(bigQueryStep3ElapsedTime);
        COLLECT(bigQueryStep4ElapsedTime);
        COLLECT(bigQueryStep5ElapsedTime);
        COLLECT(bigQueryStep6ElapsedTime);
        total->temp1 += stats->temp1;
        total->temp2 += stats->temp2;
        total->temp3 += stats->temp3;
        total->temp4 += stats->temp4;
        total->temp5 += stats->temp5;
    }
}

/**
 * Given two collections of cluster PerfStats, computes the changes from
 * the first collection to the second and formats it for printing.
 *
 * \param first
 *      Contains the response buffer from a call to
 *      CoordinatorClient::serverControlAll (i.e. PerfStats from all
 *      of the servers in the cluster at a particular point in time).
 * \param second
 *      Contains the response buffer from another call to
 *      CoordinatorClient::serverControlAll (i.e. PerfStats from all
 *      of the servers in the cluster at a point in time later than
 *      that for first).
 *
 * \return
 *      A multi-line string containing a human-readable description
 *      describing what happened between the first and second readings.
 *      The string ends in a newline character.
 */
string
PerfStats::printClusterStats(Buffer* first, Buffer* second, int numServers)
{
    string result;
    Diff diff;
    clusterDiff(first, second, numServers, &diff);
    if (diff["serverId"].size() == 0) {
        return "Insufficient PerfStats data\n";
    }

    // Create a few auxiliary rows in the data table.
    string topColumn = format(" %8s", "Avg") + format(" %8s", "Min") +
            format(" %8s", "Max");
    for (size_t i = 0; i < diff["serverId"].size(); i++) {
        diff["readBytesObjectsAndKeys"].push_back(
                diff["readObjectBytes"][i] + diff["readKeyBytes"][i]);
        diff["writeBytesObjectsAndKeys"].push_back(
                diff["writeObjectBytes"][i] + diff["writeKeyBytes"][i]);
        topColumn += format(" %8.0f", diff["serverId"][i]);
    }

    result.append(format("%-40s %s\n", "Server index",
            topColumn.c_str()).c_str());
//    result.append(format("%-40s %s\n", "Server index",
//            formatMetric(&diff, "serverId", " %8.0f").c_str()));

    result.append(format("%-40s %s\n", "Elapsed time (sec)",
            formatMetricRatio(&diff, "collectionTime", "cyclesPerSecond",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "Dispatcher load factor",
            formatMetricRatio(&diff, "dispatchActiveCycles", "collectionTime",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "Worker load factor",
            formatMetricRatio(&diff, "workerActiveCycles", "collectionTime",
            " %8.3f").c_str()));
/*
    result.append("\nReads:\n");
    result.append(format("%-40s %s\n", "  Objects read (K)",
            formatMetric(&diff, "readCount", " %8.1f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Total MB (objects & keys)",
            formatMetric(&diff, "readBytesObjectsAndKeys",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Average object size (bytes)",
            formatMetricRatio(&diff, "readObjectBytes", "readCount",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Average key data (bytes)",
            formatMetricRatio(&diff, "readKeyBytes", "readCount",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Objects/second (K)",
            formatMetricRate(&diff, "readCount", " %8.1f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Total MB/s (objects & keys)",
            formatMetricRate(&diff, "readBytesObjectsAndKeys",
            " %8.2f", 1e-6).c_str()));

    result.append("\nWrites:\n");
    result.append(format("%-40s %s\n", "  Objects written (K)",
            formatMetric(&diff, "writeCount", " %8.1f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Total MB (objects & keys)",
            formatMetric(&diff, "writeBytesObjectsAndKeys",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Average object size (bytes)",
            formatMetricRatio(&diff, "writeObjectBytes", "writeCount",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Average key data (bytes)",
            formatMetricRatio(&diff, "writeKeyBytes", "writeCount",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Objects/second (K)",
            formatMetricRate(&diff, "writeCount", " %8.1f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Total MB/s (objects & keys)",
            formatMetricRate(&diff, "writeBytesObjectsAndKeys",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Log bytes appended (MB/s)",
            formatMetricRate(&diff, "logBytesAppended",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Replication RPCs/write",
            formatMetricRatio(&diff, "replicationRpcs", "writeCount",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Log sync load factor",
            formatMetricRatio(&diff, "logSyncCycles",
            "collectionTime", " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Segment unopened time (%)",
            formatMetricRatio(&diff, "segmentUnopenedCycles",
            "collectionTime", " %8.2f", 100).c_str()));

    result.append("\nLog cleaner:\n");
    result.append(format("%-40s %s\n", "  Compactor load factor",
            formatMetricRatio(&diff, "compactorActiveCycles",
            "collectionTime", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  Cleaner load factor",
            formatMetricRatio(&diff, "cleanerActiveCycles",
            "collectionTime", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  Compactor input (MB)",
            formatMetric(&diff, "compactorInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Compactor survivor data (MB)",
            formatMetric(&diff, "compactorSurvivorBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Compactor utilization",
            formatMetricRatio(&diff, "compactorSurvivorBytes",
            "compactorInputBytes", " %8.3f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cleaner memory input (MB)",
            formatMetric(&diff, "cleanerInputMemoryBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cleaner disk input (MB)",
            formatMetric(&diff, "cleanerInputDiskBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cleaner survivor data (MB)",
            formatMetric(&diff, "cleanerSurvivorBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cleaner utilization",
            formatMetricRatio(&diff, "cleanerSurvivorBytes",
            "cleanerInputDiskBytes", " %8.3f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cleaner survivor rate (MB/s)",
            formatMetricRate(&diff, "cleanerSurvivorBytes",
            " %8.2f", 1e-6).c_str()));

    result.append("\nIndex B+ Tree Operations:\n");
    result.append(format("%-40s %s\n", "  Node reads",
            formatMetric(&diff, "btreeNodeReads", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Node writes",
            formatMetric(&diff, "btreeNodeWrites", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Bytes read for nodes (KB)",
            formatMetric(&diff, "btreeBytesRead", " %8.3f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Bytes written for nodes (KB)",
            formatMetric(&diff, "btreeBytesWritten", " %8.3f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Node splits",
            formatMetric(&diff, "btreeNodeSplits", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Node coalesces",
            formatMetric(&diff, "btreeNodeCoalesces", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Node re-balances",
            formatMetric(&diff, "btreeRebalances", " %8.0f").c_str()));

    result.append("\nBackup service:\n");
    result.append(format("%-40s %s\n", "  Backup bytes received (MB/s)",
            formatMetricRate(&diff, "backupBytesReceived",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Storage writes (MB/s)",
            formatMetricRate(&diff, "backupWriteBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Storage write ops/sec",
            formatMetricRate(&diff, "backupWriteOps",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  KB per storage write",
            formatMetricRatio(&diff, "backupWriteBytes", "backupWriteOps",
            " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Storage write load factor",
            formatMetricRatio(&diff, "backupWriteActiveCycles",
            "collectionTime", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  Storage reads (MB/s)",
            formatMetricRate(&diff, "backupReadBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Storage read ops/sec",
            formatMetricRate(&diff, "backupReadOps",
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  KB per storage read",
            formatMetricRatio(&diff, "backupReadBytes", "backupReadOps",
            " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Storage read load factor",
            formatMetricRatio(&diff, "backupReadActiveCycles",
            "collectionTime", " %8.3f").c_str()));

    result.append("\nMigration:\n");
    result.append(format("%-40s %s\n", "  P1 migrated bytes (MB/s)",
            formatMetricRate(&diff, "migrationPhase1Bytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  P1 load factor",
            formatMetricRatio(&diff, "migrationPhase1Cycles",
            "collectionTime", " %8.3f").c_str()));
*/

    auto perItemCostComputer = [] (vector<double>& v) {
        return v[1] < 1 ? 0 : v[0]/(v[1]*v[2]);
    };

#define DISPLAY_NETWORK_STATS 0
#if DISPLAY_NETWORK_STATS
    result.append("\nNetwork:\n");
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "networkInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "networkOutputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Input packets (Mpps)",
            formatMetricRate(&diff, "networkInputPackets",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output packets (Mpps)",
            formatMetricRate(&diff, "networkOutputPackets",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Average input packet size (B)",
            formatMetricRatio(&diff, "networkInputBytes",
            "networkInputPackets", " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Average output packet size (B)",
            formatMetricRatio(&diff, "networkOutputBytes",
            "networkOutputPackets", " %8.1f").c_str()));

    result.append("\nTransport:\n");
    result.append(format("%-40s %s\n", "  Basic load factor",
            formatMetricRatio(&diff, "basicTransportActiveCycles",
            "collectionTime", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  Basic input data bytes (MB/s)",
            formatMetricRate(&diff, "basicTransportInputDataBytes",
            "basicTransportActiveCycles", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Basic output data bytes (MB/s)",
            formatMetricRate(&diff, "basicTransportOutputDataBytes",
            "basicTransportActiveCycles", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Basic active time (sec)",
            formatMetricRatio(&diff, "basicTransportActiveCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    Basic send data packets (sec)",
            formatMetricRatio(&diff, "basicTransportSendDataCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "      Cost per packet  (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"basicTransportSendDataCycles", "basicTransportOutputDataPackets",
            "cyclesPerNanos"}, " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "      Packets (M)",
            formatMetric(&diff, "basicTransportOutputDataPackets",
            " %8.3f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "      Avg. packet size (B)",
            formatMetricRatio(&diff, "basicTransportOutputDataBytes",
            "basicTransportOutputDataPackets", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "    Basic send control packets (sec)",
            formatMetricRatio(&diff, "basicTransportSendControlCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "      Cost per packet (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"basicTransportSendControlCycles", "basicTransportOutputControlPackets",
            "cyclesPerNanos"}, " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "      Packets (M)",
            formatMetric(&diff, "basicTransportOutputControlPackets",
            " %8.3f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "      Avg. packet size (B)",
            formatMetricRatio(&diff, "basicTransportOutputControlBytes",
            "basicTransportOutputControlPackets", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "    Basic receive packets (sec)",
            formatMetricRatio(&diff, "basicTransportReceiveCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "      Cost per packet (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"basicTransportReceiveCycles", "basicTransportInputPackets",
            "cyclesPerNanos"}, " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "      Avg. packet size (B)",
            formatMetricRatio(&diff, "basicTransportInputDataBytes",
            "basicTransportInputPackets", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "      Packets (M)",
            formatMetric(&diff, "basicTransportInputPackets",
            " %8.3f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "    Basic handle packets (sec)",
            formatMetricRatio(&diff, "basicTransportHandlePacketCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "      Cost per packet (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"basicTransportHandlePacketCycles", "basicTransportInputPackets",
             "cyclesPerNanos"}, " %8.1f").c_str()));

    // TODO: Perf. numbers in the driver section are not very trustworthy and
    // require further investigation. The numbers tend to fluctuate a lot (!)
    // between different nodes and different runs for unknown reasons.
    result.append("\nDriver:\n");
    result.append("===WARNING===: Performance numbers in this section need "
            "further verification; they are not even stable across nodes "
            "and/or runs.\n");
    result.append(format("%-40s %s\n", "  infud send time (sec)",
            formatMetricRatio(&diff, "infudDriverTxCycles", "cyclesPerSecond",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    prepare work request (sec)",
            formatMetricRatio(&diff, "infudDriverTxPrepareCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    ibv_post_send (sec)",
            formatMetricRatio(&diff, "infudDriverTxPostSendCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    post-processing (sec)",
            formatMetricRatio(&diff, "infudDriverTxPostProcessCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  infud recv time (sec)",
            formatMetricRatio(&diff, "infudDriverRxCycles", "cyclesPerSecond",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    ibv_poll_cq (sec)",
            formatMetricRatio(&diff, "infudDriverRxPollCqCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    prefetch + refill (sec)",
            formatMetricRatio(&diff, "infudDriverRxRefillCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "    process packets (sec)",
            formatMetricRatio(&diff, "infudDriverRxProcessPacketCycles",
            "cyclesPerSecond", " %8.3f").c_str()));
#endif

    auto vs = diff.find("bigQueryTime")->second;
    if (std::accumulate(vs.begin(), vs.end(), 0.0) > 1e-6) {
        result.append("\nBigQuery:\n");
        result.append("\n=== Total ===\n");
        result.append(format("%-40s %s\n", "  Elapsed time (ms)",
                formatMetricRatio(&diff, "bigQueryTime", "cyclesPerMillis",
                " %8.3f").c_str()));

        result.append("\n=== Time Breakdown (indiv. server) ===\n");
        result.append(format("%-40s %s\n", "  Step 1 (ms)",
                formatMetricRatio(&diff, "bigQueryStep1ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        result.append(format("%-40s %s\n", "  Step 2 (ms)",
                formatMetricRatio(&diff, "bigQueryStep2ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        result.append(format("%-40s %s\n", "  Step 3 (ms)",
                formatMetricRatio(&diff, "bigQueryStep3ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        result.append(format("%-40s %s\n", "  Step 4 (ms)",
                formatMetricRatio(&diff, "bigQueryStep4ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        result.append(format("%-40s %s\n", "  Step 5 (ms)",
                formatMetricRatio(&diff, "bigQueryStep5ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        result.append(format("%-40s %s\n", "  Step 6 (ms)",
                formatMetricRatio(&diff, "bigQueryStep6ElapsedTime",
                "cyclesPerMillis", " %8.3f").c_str()));
        return result;
    }

    // TODO: Arachne core util.
    vs = diff.find("millisortTime")->second;
    if (std::accumulate(vs.begin(), vs.end(), 0.0) < 1e-6) {
        return result;
    }
    result.append("\nMilliSort:\n");
    result.append("\n=== Total ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (ms)",
            formatMetricRatio(&diff, "millisortTime", "cyclesPerMillis",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "  Initial items",
            formatMetric(&diff, "millisortInitItems", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Final items",
            formatMetric(&diff, "millisortFinalItems", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Imbalance ratio",
            formatMetricRatio(&diff, "millisortFinalItems",
            "millisortInitItems", " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Cost per item (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"millisortTime", "millisortFinalItems", "cyclesPerNanos"},
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Initial keys (MB)",
            formatMetric(&diff, "millisortInitKeyBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Initial values (MB)",
            formatMetric(&diff, "millisortInitValueBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Is PivotSorter?",
            formatMetric(&diff, "millisortIsPivotSorter", " %8.0f").c_str()));

    result.append("\n=== Time Breakdown (indiv. server) ===\n");
    result.append(format("%-40s %s\n", "  Local sorting (us)",
            formatMetricRatio(&diff, "localSortElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Rearrange ini. values (overlaped) (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Partitioning (overall) (us)",
            formatMetricRatio(&diff, "partitionElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Gather pivots (us)",
            formatMetricRatio(&diff, "gatherPivotsElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Gather super-pivots (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Bcast pivot bucket boundaries (us)",
            formatMetricRatio(&diff, "bcastPivotBucketBoundariesElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Shuffle pivots (us)",
            formatMetricRatio(&diff, "shufflePivotsElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    All-gather & bcast pivots (us)",
            formatMetricRatio(&diff, "allGatherPivotsElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Shuffle records (us)",
            formatMetricRatio(&diff, "shuffleKeysElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Online merge-sort (us)",
            formatMetricRatio(&diff, "onlineMergeSortElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Rearrange final values (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Total time (us)",
            formatMetricRatio(&diff, "millisortTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Sort+Part.+Shuffle+Rearrange (us)",
            formatMetricLambda(&diff,
            [] (vector<double>& v) {
                double sum = v[0] + std::max(v[1], v[2]) + v[3] + v[4] + v[5];
                return (sum - v.back()) / v.back();
            },
            {"localSortElapsedTime",
             "rearrangeInitValuesElapsedTime",
             "partitionElapsedTime",
             "shuffleKeysElapsedTime",
             "onlineMergeSortElapsedTime",
             "rearrangeFinalValuesElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Unaccounted for (us)",
            formatMetricLambda(&diff,
            [] (vector<double>& v) {
                double sum = v[0] -
                        (v[1] + std::max(v[2], v[3]) + v[4] + v[5] + v[6]);
                return (sum - v.back()) / v.back();
            },
            {"millisortTime",
             "localSortElapsedTime",
             "rearrangeInitValuesElapsedTime",
             "partitionElapsedTime",
             "shuffleKeysElapsedTime",
             "onlineMergeSortElapsedTime",
             "rearrangeFinalValuesElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));

    result.append("\n=== Time Breakdown (cluster) ===\n");
    auto adjustElapsedTime = [] (vector<vector<double>>& vs, int serverId) {
        // The first metric is startTime; the last metric is cyclesPerMicros.
        // Metrics in between are elapsed times for one or more steps.
        assert(vs.size() >= 3);
        double latestStartTime = 0.0;
        for (double startTime : vs[0]) {
            latestStartTime = std::max(latestStartTime, startTime);
        }
        if (vs[0][serverId] < 1e-3) {
            return 0.0;
        } else {
            double stopTime = 0.0;
            for (int i = 0; i < vs.size() - 1; i++) {
                stopTime += vs[i][serverId];
            }
            return stopTime > latestStartTime ?
                    (stopTime - latestStartTime) / vs.back()[0] : 0.0;
        }
    };
    result.append(format("%-40s %s\n", "  Local sorting (us)",
            formatMetricRatio(&diff, "localSortElapsedTime", "cyclesPerMicros",
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Rearrange (overlap) (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"rearrangeInitValuesStartTime", "rearrangeInitValuesElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Partitioning (overall) (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"gatherPivotsStartTime", "partitionElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    Shuffle pivots (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"shufflePivotsStartTime", "shufflePivotsElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Shuffle records (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"shuffleKeysStartTime", "shuffleKeysElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  MergeSort + Rearrange (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"onlineMergeSortStartTime", "onlineMergeSortElapsedTime",
             "rearrangeFinalValuesElapsedTime", "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "    MergeSort (us)",
            formatMetricGeneric(&diff, adjustElapsedTime,
            {"onlineMergeSortStartTime", "onlineMergeSortElapsedTime",
             "cyclesPerMicros"},
            " %8.2f").c_str()));
    result.append(format("%-40s %s\n", "  Total time (us)",
            formatMetricRatio(&diff, "millisortTime", "cyclesPerMicros",
            " %8.2f").c_str()));

    result.append("\n=== Local Sorting ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "localSortStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "localSortElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "localSortElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "localSortWorkers", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Cost per key (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"localSortElapsedTime", "millisortInitItems", "cyclesPerNanos"},
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Sort rate (10^6 keys/s)",
            formatMetricLambda(&diff,
            [] (vector<double>& v) { return v[0]/(v[1]/v[2]); },
            {"millisortInitItems", "localSortElapsedTime", "cyclesPerMicros"},
            " %8.2f").c_str()));

    result.append("\n=== Rearrange Initial Values (overlapped) ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "rearrangeInitValuesElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  CPU time (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesCycles",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "rearrangeInitValuesWorkers", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Move rate (10^6 values/s)",
            formatMetricRate(&diff, "millisortInitItems",
            "rearrangeInitValuesElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Move rate (MB/s)",
            formatMetricRate(&diff, "millisortInitValueBytes",
            "rearrangeInitValuesElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cost per value (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"rearrangeInitValuesElapsedTime", "millisortInitItems", "cyclesPerNanos"},
            " %8.1f").c_str()));
    result.append(format("%-40s %s\n", "  Slack time (pre. \"Shuffle Rec.\") (us)",
            formatMetricLambda(&diff,
            [] (vector<double>& v) { return (v[2]-(v[0]+v[1]))/v[3]; },
            {"rearrangeInitValuesStartTime", "rearrangeInitValuesElapsedTime",
            "shuffleKeysStartTime", "cyclesPerMicros"}, " %8.0f").c_str()));

    result.append("\n=== Gather Pivots ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "gatherPivotsStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "gatherPivotsElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "gatherPivotsElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (KB)",
            formatMetric(&diff, "gatherPivotsInputBytes", " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (KB)",
            formatMetric(&diff, "gatherPivotsOutputBytes", " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "gatherPivotsInputBytes",
            "gatherPivotsElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "gatherPivotsOutputBytes",
            "gatherPivotsElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "gatherPivotsMergeCycles", "cyclesPerMicros",
            " %8.0f").c_str()));

    result.append("\n=== Gather Super-Pivots ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "gatherSuperPivotsElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (KB)",
            formatMetric(&diff, "gatherSuperPivotsInputBytes", " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (KB)",
            formatMetric(&diff, "gatherSuperPivotsOutputBytes", " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "gatherSuperPivotsInputBytes",
            "gatherSuperPivotsElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "gatherSuperPivotsOutputBytes",
            "gatherSuperPivotsElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsMergeCycles", "cyclesPerMicros",
            " %8.0f").c_str()));

    result.append("\n=== Broadcast Pivot Bucket Boundaries ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "bcastPivotBucketBoundariesStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "bcastPivotBucketBoundariesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "bcastPivotBucketBoundariesElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));

    result.append("\n=== Shuffle Pivots ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "shufflePivotsStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "shufflePivotsElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "shufflePivotsElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "mergePivotsInBucketSortCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== All-Gather Data Bucket Boundaries ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "allGatherPivotsStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "allGatherPivotsElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "allGatherPivotsElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "allGatherPivotsMergeCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Shuffle Records ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "shuffleKeysStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "shuffleKeysElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "shuffleKeysElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB)",
            formatMetric(&diff, "shuffleKeysInputBytes", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB)",
            formatMetric(&diff, "shuffleKeysOutputBytes", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Input rate (Gbps)",
            formatMetricRate(&diff, "shuffleKeysInputBytes",
            "shuffleKeysElapsedTime", " %8.2f", 8e-9).c_str()));
    result.append(format("%-40s %s\n", "  Output rate (Gbps)",
            formatMetricRate(&diff, "shuffleKeysOutputBytes",
            "shuffleKeysElapsedTime", " %8.2f", 8e-9).c_str()));
    result.append(format("%-40s %s\n", "  Received RPCs",
            formatMetric(&diff, "shuffleKeysReceivedRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Sent RPCs",
            formatMetric(&diff, "shuffleKeysSentRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Avg. message size (KB)",
            formatMetricRatio(&diff, "shuffleKeysOutputBytes",
            "millisortNodes", " %8.2f", 1e-3).c_str()));
    result.append(format("%-40s %s\n", "  Avg. chunk size (KB)",
            formatMetricRatio(&diff, "shuffleKeysOutputBytes",
            "shuffleKeysSentRpcs", " %8.2f", 1e-3).c_str()));
    // TODO: # parallel shuffle key pullers
    result.append(format("%-40s %s\n", "  Copy RPC response (us)",
            formatMetricRatio(&diff, "shuffleKeysCopyResponseCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Online MergeSort ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "onlineMergeSortStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elasped time (us)",
            formatMetricRatio(&diff, "onlineMergeSortElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "onlineMergeSortElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    // TODO: active time
    // TODO: CPU time
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "onlineMergeSortWorkers", " %8.0f").c_str()));

    result.append("\n=== Rearrange Values ===\n");
    result.append(format("%-40s %s\n", "  Start time (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesStartTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Elasped time (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Percentage (%)",
            formatMetricRatio(&diff, "rearrangeFinalValuesElapsedTime", "millisortTime",
            " %8.2f", 100).c_str()));
    result.append(format("%-40s %s\n", "  CPU time (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesCycles",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "rearrangeFinalValuesWorkers", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Move rate (10^6 values/s)",
            formatMetricRate(&diff, "millisortFinalItems",
            "rearrangeFinalValuesElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Move rate (MB/s)",
            formatMetricRate(&diff, "millisortFinalValueBytes",
            "rearrangeFinalValuesElapsedTime", " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Cost per value (ns)",
            formatMetricLambda(&diff, perItemCostComputer,
            {"rearrangeFinalValuesElapsedTime", "millisortFinalItems", "cyclesPerNanos"},
            " %8.1f").c_str()));

    return result;
}

/**
 * Given the raw response data returned by two calls to
 * CoordinatorClient::serverControlAll, return information about
 * how much each individual metric change between the two calls,
 * for each server.
 *
 * \param before
 *      Response buffer from a call to CoordinatorClient::serverControlAll.
 * \param after
 *      Response buffer from a later call to
 *      CoordinatorClient::serverControlAll.
 * \param numServers
 *      # servers that participate in the experiment. They must appear first
 *      in the two perfstats buffers.
 * \param[out] diff
 *      Contents are replaced with information about how much each
 *      performance metric changed between the before and after
 *      measurements. See the declaration of Diff for details
 *      on the format of this information.
 */
void
PerfStats::clusterDiff(Buffer* before, Buffer* after, int numServers,
        PerfStats::Diff* diff)
{
    // First, parse each of the two readings.
    std::vector<PerfStats> firstStats, secondStats;
    parseStats(before, &firstStats);
    parseStats(after, &secondStats);

    // Each iteration of the following loop processes one server, appending
    // information to the response.
    for (size_t i = 0; i < firstStats.size(); i++) {
        // Make sure we have data from both readings.
        if (i >= secondStats.size()) {
            break;
        }
        PerfStats& p1 = firstStats[i];
        PerfStats& p2 = secondStats[i];
        if ((p1.collectionTime == 0) || (p2.collectionTime == 0)){
            continue;
        }

        // Ignore servers that don't participate in the experiment.
        if (static_cast<int>((*diff)["serverId"].size()) == numServers) {
            break;
        }

#define ADD_METRIC(metric) \
        (*diff)[#metric].push_back(static_cast<double>(p2.metric - p1.metric))

        // Collect data for each of the metrics. The order below should
        // match the declaration order in PerfStats.h.
        (*diff)["serverId"].push_back(static_cast<double>(i));
        (*diff)["cyclesPerSecond"].push_back(p1.cyclesPerSecond);
        (*diff)["cyclesPerMillis"].push_back(p1.cyclesPerSecond * 1e-3);
        (*diff)["cyclesPerMicros"].push_back(p1.cyclesPerSecond * 1e-6);
        (*diff)["cyclesPerNanos"].push_back(p1.cyclesPerSecond * 1e-9);
        ADD_METRIC(collectionTime);
        ADD_METRIC(readCount);
        ADD_METRIC(readObjectBytes);
        ADD_METRIC(readKeyBytes);
        ADD_METRIC(writeCount);
        ADD_METRIC(writeObjectBytes);
        ADD_METRIC(writeKeyBytes);
        ADD_METRIC(dispatchActiveCycles);
        ADD_METRIC(workerActiveCycles);
        ADD_METRIC(btreeNodeReads);
        ADD_METRIC(btreeNodeWrites);
        ADD_METRIC(btreeBytesRead);
        ADD_METRIC(btreeBytesWritten);
        ADD_METRIC(btreeNodeSplits);
        ADD_METRIC(btreeNodeCoalesces);
        ADD_METRIC(btreeRebalances);
        ADD_METRIC(logBytesAppended);
        ADD_METRIC(replicationRpcs);
        ADD_METRIC(logSyncCycles);
        ADD_METRIC(segmentUnopenedCycles);
        ADD_METRIC(compactorInputBytes);
        ADD_METRIC(compactorSurvivorBytes);
        ADD_METRIC(compactorActiveCycles);
        ADD_METRIC(cleanerInputMemoryBytes);
        ADD_METRIC(cleanerInputDiskBytes);
        ADD_METRIC(cleanerSurvivorBytes);
        ADD_METRIC(cleanerActiveCycles);
        ADD_METRIC(backupReadOps);
        ADD_METRIC(backupReadBytes);
        ADD_METRIC(backupReadActiveCycles);
        ADD_METRIC(backupBytesReceived);
        ADD_METRIC(backupWriteOps);
        ADD_METRIC(backupWriteBytes);
        ADD_METRIC(backupWriteActiveCycles);
        ADD_METRIC(migrationPhase1Bytes);
        ADD_METRIC(migrationPhase1Cycles);
        ADD_METRIC(networkInputBytes);
        ADD_METRIC(networkOutputBytes);
        ADD_METRIC(networkInputPackets);
        ADD_METRIC(networkOutputPackets);
        ADD_METRIC(basicTransportActiveCycles);
        ADD_METRIC(basicTransportHandlePacketCycles);
        ADD_METRIC(basicTransportReceiveCycles);
        ADD_METRIC(basicTransportInputPackets);
        ADD_METRIC(basicTransportInputDataBytes);
        ADD_METRIC(basicTransportSendDataCycles);
        ADD_METRIC(basicTransportSendControlCycles);
        ADD_METRIC(basicTransportOutputControlPackets);
        ADD_METRIC(basicTransportOutputControlBytes);
        ADD_METRIC(basicTransportOutputDataBytes);
        ADD_METRIC(basicTransportOutputDataPackets);
        ADD_METRIC(infudDriverTxCycles);
        ADD_METRIC(infudDriverTxPrepareCycles);
        ADD_METRIC(infudDriverTxPostSendCycles);
        ADD_METRIC(infudDriverTxPostProcessCycles);
        ADD_METRIC(infudDriverRxCycles);
        ADD_METRIC(infudDriverRxPollCqCycles);
        ADD_METRIC(infudDriverRxRefillCycles);
        ADD_METRIC(infudDriverRxProcessPacketCycles);
        ADD_METRIC(millisortNodes);
        ADD_METRIC(millisortTime);
        ADD_METRIC(millisortInitItems);
        ADD_METRIC(millisortFinalItems);
        ADD_METRIC(millisortInitKeyBytes);
        ADD_METRIC(millisortFinalPivotKeyBytes);
        ADD_METRIC(millisortInitValueBytes);
        ADD_METRIC(millisortFinalValueBytes);
        ADD_METRIC(millisortIsPivotSorter);
        ADD_METRIC(localSortStartTime);
        ADD_METRIC(localSortElapsedTime);
        ADD_METRIC(localSortWorkers);
        ADD_METRIC(rearrangeInitValuesStartTime);
        ADD_METRIC(rearrangeInitValuesElapsedTime);
        ADD_METRIC(rearrangeInitValuesCycles);
        ADD_METRIC(rearrangeInitValuesWorkers);
        ADD_METRIC(partitionElapsedTime);
        ADD_METRIC(gatherPivotsStartTime);
        ADD_METRIC(gatherPivotsElapsedTime);
        ADD_METRIC(gatherPivotsOutputBytes);
        ADD_METRIC(gatherPivotsInputBytes);
        ADD_METRIC(gatherPivotsMergeCycles);
        ADD_METRIC(gatherSuperPivotsStartTime);
        ADD_METRIC(gatherSuperPivotsElapsedTime);
        ADD_METRIC(gatherSuperPivotsOutputBytes);
        ADD_METRIC(gatherSuperPivotsInputBytes);
        ADD_METRIC(gatherSuperPivotsMergeCycles);
        ADD_METRIC(bcastPivotBucketBoundariesStartTime);
        ADD_METRIC(bcastPivotBucketBoundariesElapsedTime);
        ADD_METRIC(shufflePivotsStartTime);
        ADD_METRIC(shufflePivotsElapsedTime);
        ADD_METRIC(mergePivotsInBucketSortCycles);
        ADD_METRIC(allGatherPivotsStartTime);
        ADD_METRIC(allGatherPivotsElapsedTime);
        ADD_METRIC(allGatherPivotsMergeCycles);
        ADD_METRIC(shuffleKeysStartTime);
        ADD_METRIC(shuffleKeysElapsedTime);
        ADD_METRIC(shuffleKeysInputBytes);
        ADD_METRIC(shuffleKeysOutputBytes);
        ADD_METRIC(shuffleKeysReceivedRpcs);
        ADD_METRIC(shuffleKeysSentRpcs);
        ADD_METRIC(shuffleKeysCopyResponseCycles);
        ADD_METRIC(onlineMergeSortStartTime);
        ADD_METRIC(onlineMergeSortElapsedTime);
        ADD_METRIC(onlineMergeSortWorkers);
        ADD_METRIC(rearrangeFinalValuesStartTime);
        ADD_METRIC(rearrangeFinalValuesElapsedTime);
        ADD_METRIC(rearrangeFinalValuesCycles);
        ADD_METRIC(rearrangeFinalValuesWorkers);
        ADD_METRIC(bigQueryTime);
        ADD_METRIC(bigQueryStep1ElapsedTime);
        ADD_METRIC(bigQueryStep2ElapsedTime);
        ADD_METRIC(bigQueryStep3ElapsedTime);
        ADD_METRIC(bigQueryStep4ElapsedTime);
        ADD_METRIC(bigQueryStep5ElapsedTime);
        ADD_METRIC(bigQueryStep6ElapsedTime);
        ADD_METRIC(temp1);
        ADD_METRIC(temp2);
        ADD_METRIC(temp3);
        ADD_METRIC(temp4);
        ADD_METRIC(temp5);
    }
}

/**
 * Given the raw response returned by CoordinatorClient::serverControlAll,
 * divide it up into individual PerfStats objects for each server, and
 * store those in an array indexed by server id.
 *
 * \param rawData
 *      Response buffer from a call to CoordinatorClient::serverControlAll.
 * \param[out] results
 *      Filled in (possibly sparsely) with contents parsed from rawData.
 *      Entry i will contain PerfStats for the server whose ServerId has
 *      indexNumber i. Empty entries have 0 collectionTimes.
 */
void
PerfStats::parseStats(Buffer* rawData, std::vector<PerfStats>* results)
{
    results->clear();
    uint32_t offset = sizeof(WireFormat::ServerControlAll::Response);
    while (offset < rawData->size()) {
        WireFormat::ServerControl::Response* header =
                rawData->getOffset<WireFormat::ServerControl::Response>(offset);
        offset += sizeof32(*header);
        if ((header == NULL) ||
                ((offset + sizeof32(PerfStats)) > rawData->size())) {
            break;
        }
        uint32_t i = ServerId(header->serverId).indexNumber();
        if (i >= results->size()) {
            results->resize(i+1);
        }
        rawData->copy(offset, header->outputLength, &results->at(i));
        offset += header->outputLength;
    }
}

/**
 * Generates a formatted string containing the values of a particular metric
 * for each of the servers.
 *
 * \param diff
 *      A collection of performance metrics, as returned by
 *      PerfStats::clusterDiff.
 * \param metric
 *      String name of a particular metric (one of the fields of
 *      PerfStats).
 * \param formatString
 *      Printf-style format string that determines how each value of
 *      the metric is formatted. Should contain a single format
 *      specifier that will work with double values, such as "%f".
 * \param scale
 *      Optional scale factor; if specified, each metric will be
 *      multiplied by this value before printing.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetric(PerfStats::Diff* diff, const char* metric,
        const char* formatString, double scale)
{
    string result;
    if (diff->find(metric) == diff->end()) {
        return format("no metric %s", metric);
    }
    vector<double>& v = (*diff)[metric];
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    foreach (double value, v) {
        value *= scale;
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetricLambda(PerfStats::Diff* diff,
        std::function<double(vector<double>&)> compute,
        vector<const char*> metrics, const char* formatString)
{
    string result;
    for (auto& metric : metrics) {
        if (diff->find(metric) == diff->end()) {
            return format("no metric %s", metric);
        }
    }
    size_t numServers = (*diff)[metrics.front()].size();
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    for (size_t i = 0; i < numServers; i++) {
        vector<double> vs;
        for (auto& metric : metrics) {
            vs.push_back((*diff)[metric][i]);
        }
        double value = compute(vs);
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetricGeneric(PerfStats::Diff* diff,
        std::function<double(vector<vector<double>>&,int)> compute,
        vector<const char*> metrics, const char* formatString)
{
    string result;
    for (auto& metric : metrics) {
        if (diff->find(metric) == diff->end()) {
            return format("no metric %s", metric);
        }
    }

    // Collect all metrics from all servers into a vector of vectors.
    size_t numServers = (*diff)[metrics.front()].size();
    vector<vector<double>> vs;
    for (auto& metric : metrics) {
        vs.emplace_back();
        for (size_t i = 0; i < numServers; i++) {
            vs.back().push_back((*diff)[metric][i]);
        }
    }

    // Each iteration of the following loop computes the derived metric for one
    // server.
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    for (size_t i = 0; i < numServers; i++) {
        double value = compute(vs, i);
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

/**
 * Generates a formatted string containing the rate per second of a given
 * metric, for each of the servers.
 *
 * \param diff
 *      A collection of performance metrics, as returned by
 *      PerfStats::clusterDiff.
 * \param metric
 *      String name of a metric (one of the fields of PerfStats). The value
 *      of this metric will be divided by elapsed time (computed from the
 *      "collectionTime" and "cyclesPerSecond" metrics) for formatting.
 * \param formatString
 *      Printf-style format string that determines how each ratio
 *      is to be formatted. Should contain a single format specifier
 *      that will work with double values, such as "%f".
 * \param scale
 *      Optional scale factor; if specified, each rate will be multiplied
 *      by this value before printing.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetricRate(PerfStats::Diff* diff, const char* metric,
        const char* formatString, double scale)
{
    string result;
    if (diff->find(metric) == diff->end()) {
        return format("no metric %s", metric);
    }
    if (diff->find("collectionTime") == diff->end()) {
        return "no metric collectionTime";
    }
    if (diff->find("cyclesPerSecond") == diff->end()) {
        return "no metric cyclesPerSecond";
    }
    vector<double>& v = (*diff)[metric];
    vector<double>& cycles = (*diff)["collectionTime"];
    vector<double>& cyclesPerSecond = (*diff)["cyclesPerSecond"];
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    for (size_t i = 0; i < v.size(); i++) {
        double value = 0;
        if (cycles[i] != 0) {
            value = v[i]/(cycles[i]/cyclesPerSecond[i]);
        }
        value *= scale;
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetricRate(PerfStats::Diff* diff, const char* metric,
        const char* elapsedTime, const char* formatString, double scale)
{
    string result;
    if (diff->find(metric) == diff->end()) {
        return format("no metric %s", metric);
    }
    if (diff->find(elapsedTime) == diff->end()) {
        return format("no metric %s", elapsedTime);
    }
    if (diff->find("cyclesPerSecond") == diff->end()) {
        return "no metric cyclesPerSecond";
    }
    vector<double>& v = (*diff)[metric];
    vector<double>& cycles = (*diff)[elapsedTime];
    vector<double>& cyclesPerSecond = (*diff)["cyclesPerSecond"];
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    for (size_t i = 0; i < v.size(); i++) {
        double value = 0;
        if (cycles[i] != 0) {
            value = v[i]/(cycles[i]/cyclesPerSecond[i]);
        }
        value *= scale;
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

/**
 * Generates a formatted string containing the ratio of two metrics for
 * each of the servers.
 *
 * \param diff
 *      A collection of performance metrics, as returned by
 *      PerfStats::clusterDiff.
 * \param metric1
 *      String name of the first (numerator) metric (one of the fields of
 *      PerfStats).
 * \param metric2
 *      String name of the second (denominator) metric (one of the fields of
 *      PerfStats).
 * \param formatString
 *      Printf-style format string that determines how each ratio
 *      is to be formatted. Should contain a single format specifier
 *      that will work with double values, such as "%f".
 * \param scale
 *      Optional scale factor; if specified, each ratio will be
 *      multiplied by this value before printing.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
string
PerfStats::formatMetricRatio(PerfStats::Diff* diff, const char* metric1,
        const char* metric2, const char* formatString, double scale)
{
    string result;
    if (diff->find(metric1) == diff->end()) {
        return format("no metric %s", metric1);
    }
    if (diff->find(metric2) == diff->end()) {
        return format("no metric %s", metric2);
    }
    vector<double>& v1 = (*diff)[metric1];
    vector<double>& v2 = (*diff)[metric2];
    double min = DOUBLE_MAX, max = -DOUBLE_MAX, avg = 0, numSamples = 0;
    for (size_t i = 0; i < v1.size(); i++) {
        double value = (v2[i] == 0) ? 0 : v1[i]/v2[i];
        value *= scale;
        updateAggMetric(avg, min, max, numSamples, value);
        result.append(format(formatString, value));
    }
    string prefix = format(formatString, avg) + format(formatString, min) +
            format(formatString, max) + " |";
    result = prefix + result;
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

void
PerfStats::updateAggMetric(double &avg, double &min, double &max,
        double& numSamples, double value)
{
    min = std::min(min, value);
    max = std::max(max, value);
    if (value > 1e-3) {
        avg = ((avg * numSamples) + value) / (numSamples + 1);
        numSamples++;
    }
}

}  // namespace RAMCloud
