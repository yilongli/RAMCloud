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

#define COLLECT(x) total->x += stats->x
        COLLECT(millisortTime);
        COLLECT(millisortInitItems);
        COLLECT(millisortFinalItems);
        COLLECT(millisortInitKeyBytes);
        COLLECT(millisortInitValueBytes);
        COLLECT(millisortFinalValueBytes);
        COLLECT(localSortElapsedTime);
        COLLECT(localSortCycles);
        total->rearrangeInitValuesElapsedTime = std::max(
                total->rearrangeInitValuesElapsedTime,
                stats->rearrangeInitValuesElapsedTime);
        COLLECT(rearrangeInitValuesCycles);
        COLLECT(rearrangeInitValuesWorkers);
        COLLECT(gatherPivotsElapsedTime);
        COLLECT(gatherPivotsInputBytes);
        COLLECT(gatherPivotsOutputBytes);
        COLLECT(gatherPivotsMergeCycles);
        COLLECT(gatherSuperPivotsElapsedTime);
        COLLECT(gatherSuperPivotsInputBytes);
        COLLECT(gatherSuperPivotsOutputBytes);
        COLLECT(gatherSuperPivotsMergeCycles);
        total->bcastPivotBucketBoundariesCycles +=
                stats->bcastPivotBucketBoundariesCycles;
        COLLECT(bucketSortPivotsElapsedTime);
        total->mergePivotsInBucketSortCycles +=
                stats->mergePivotsInBucketSortCycles;
        total->allGatherPivotsElapsedTime += stats->allGatherPivotsElapsedTime;
        total->allGatherPivotsMergeCycles += stats->allGatherPivotsMergeCycles;
        COLLECT(shuffleKeysElapsedTime);
        COLLECT(shuffleKeysInputBytes);
        COLLECT(shuffleKeysOutputBytes);
        COLLECT(shuffleKeysReceivedRpcs);
        COLLECT(shuffleKeysSentRpcs);
        COLLECT(shuffleValuesElapsedTime);
        COLLECT(shuffleValuesInputBytes);
        COLLECT(shuffleValuesOutputBytes);
        COLLECT(shuffleValuesReceivedRpcs);
        COLLECT(shuffleValuesSentRpcs);
        total->bucketSortMergeKeyCycles += stats->bucketSortMergeKeyCycles;
        total->bucketSortMergeValueCycles += stats->bucketSortMergeValueCycles;
        total->rearrangeFinalValuesElapsedTime = std::max(
                total->rearrangeFinalValuesElapsedTime,
                stats->rearrangeFinalValuesElapsedTime);
        COLLECT(rearrangeFinalValuesCycles);
        COLLECT(rearrangeFinalValuesWorkers);
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
PerfStats::printClusterStats(Buffer* first, Buffer* second)
{
    string result;
    Diff diff;
    clusterDiff(first, second, &diff);
    if (diff["serverId"].size() == 0) {
        return "Insufficient PerfStats data\n";
    }

    // Create a few auxiliary rows in the data table.
    for (size_t i = 0; i < diff["serverId"].size(); i++) {
        diff["readBytesObjectsAndKeys"].push_back(
                diff["readObjectBytes"][i] + diff["readKeyBytes"][i]);
        diff["writeBytesObjectsAndKeys"].push_back(
                diff["writeObjectBytes"][i] + diff["writeKeyBytes"][i]);
    }

    result.append(format("%-40s %s\n", "Server index",
            formatMetric(&diff, "serverId", " %8.0f").c_str()));
/*
    result.append(format("%-40s %s\n", "Elapsed time (sec)",
            formatMetricRatio(&diff, "collectionTime", "cyclesPerSecond",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "Dispatcher load factor",
            formatMetricRatio(&diff, "dispatchActiveCycles", "collectionTime",
            " %8.3f").c_str()));
    result.append(format("%-40s %s\n", "Worker load factor",
            formatMetricRatio(&diff, "workerActiveCycles", "collectionTime",
            " %8.3f").c_str()));

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

    result.append("\nNetwork:\n");
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "networkInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "networkOutputBytes",
            " %8.2f", 1e-6).c_str()));
*/

    result.append("\nMilliSort:\n");
    result.append("\n=== Total ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "millisortTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Initial items",
            formatMetric(&diff, "millisortInitItems", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Final items",
            formatMetric(&diff, "millisortFinalItems", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Cost per item (ns)",
            formatMetricRatio(&diff, "millisortTime", "millisortFinalItems",
            " %8.1f", 1e9/Cycles::perSecond()).c_str()));
    result.append(format("%-40s %s\n", "  Initial keys (MB)",
            formatMetric(&diff, "millisortInitKeyBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Initial values (MB)",
            formatMetric(&diff, "millisortInitValueBytes",
            " %8.2f", 1e-6).c_str()));

    result.append("\n=== Local Sorting ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "localSortElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  CPU time (us)",
            formatMetricRatio(&diff, "localSortCycles", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Cost per key (ns)",
            formatMetricRatio(&diff, "localSortElapsedTime", "millisortInitItems",
            " %8.1f", 1e9/Cycles::perSecond()).c_str()));

    result.append("\n=== Rearrange Initial Values (overlap) ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  CPU time (us)",
            formatMetricRatio(&diff, "rearrangeInitValuesCycles",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "rearrangeInitValuesWorkers", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Values moved (MB/s)",
            formatMetricRatio(&diff, "millisortInitValueBytes",
            "rearrangeInitValuesElapsedTime", " %8.2f", 1e3).c_str()));

    result.append("\n=== Gather Pivots ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "gatherPivotsElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "gatherPivotsInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "gatherPivotsOutputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "gatherPivotsMergeCycles", "cyclesPerMicros",
            " %8.0f").c_str()));

    result.append("\n=== Gather Super-Pivots ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsElapsedTime", "cyclesPerMicros",
            " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "gatherSuperPivotsInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "gatherSuperPivotsOutputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "gatherSuperPivotsMergeCycles", "cyclesPerMicros",
            " %8.0f").c_str()));

    result.append("\n=== Broadcast Pivot Bucket Boundaries ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "bcastPivotBucketBoundariesCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Shuffle Pivots ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "bucketSortPivotsElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "mergePivotsInBucketSortCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== All-Gather Data Bucket Boundaries ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "allGatherPivotsElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "allGatherPivotsMergeCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Shuffle Keys ===\n");
    result.append(format("%-40s %s\n", "  Elapsed time (us)",
            formatMetricRatio(&diff, "shuffleKeysElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    // FIXME: this is not right! should be divided by shuffleKeysElapsedTime
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "shuffleKeysInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "shuffleKeysOutputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Received RPCs",
            formatMetric(&diff, "shuffleKeysReceivedRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Sent RPCs",
            formatMetric(&diff, "shuffleKeysSentRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "bucketSortMergeKeyCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Online MergeSort (overlap) ===\n");
    result.append("\nTODO\n");

    result.append("\n=== Shuffle Values ===\n");
    result.append(format("%-40s %s\n", "  Elasped time (us)",
            formatMetricRatio(&diff, "shuffleValuesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Input bytes (MB/s)",
            formatMetricRate(&diff, "shuffleValuesInputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Output bytes (MB/s)",
            formatMetricRate(&diff, "shuffleValuesOutputBytes",
            " %8.2f", 1e-6).c_str()));
    result.append(format("%-40s %s\n", "  Received RPCs",
            formatMetric(&diff, "shuffleValuesReceivedRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Sent RPCs",
            formatMetric(&diff, "shuffleValuesSentRpcs", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Merge CPU time (us)",
            formatMetricRatio(&diff, "bucketSortMergeValueCycles",
            "cyclesPerMicros", " %8.0f").c_str()));

    result.append("\n=== Rearrange Values ===\n");
    result.append(format("%-40s %s\n", "  Elasped time (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesElapsedTime",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  CPU time (us)",
            formatMetricRatio(&diff, "rearrangeFinalValuesCycles",
            "cyclesPerMicros", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Parallel workers",
            formatMetric(&diff, "rearrangeFinalValuesWorkers", " %8.0f").c_str()));
    result.append(format("%-40s %s\n", "  Values moved (MB/s)",
            formatMetricRatio(&diff, "millisortFinalValueBytes",
            "rearrangeFinalValuesElapsedTime", " %8.2f", 1e3).c_str()));

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
 * \param[out] diff
 *      Contents are replaced with information about how much each
 *      performance metric changed between the before and after
 *      measurements. See the declaration of Diff for details
 *      on the format of this information.
 */
void
PerfStats::clusterDiff(Buffer* before, Buffer* after,
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

#define ADD_METRIC(metric) \
        (*diff)[#metric].push_back(static_cast<double>(p2.metric - p1.metric))

        // Collect data for each of the metrics. The order below should
        // match the declaration order in PerfStats.h.
        (*diff)["serverId"].push_back(static_cast<double>(i));
        (*diff)["cyclesPerSecond"].push_back(p1.cyclesPerSecond);
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
        ADD_METRIC(millisortTime);
        ADD_METRIC(millisortInitItems);
        ADD_METRIC(millisortFinalItems);
        ADD_METRIC(millisortInitKeyBytes);
        ADD_METRIC(millisortInitValueBytes);
        ADD_METRIC(millisortFinalValueBytes);
        ADD_METRIC(localSortElapsedTime);
        ADD_METRIC(localSortCycles);
        ADD_METRIC(rearrangeInitValuesElapsedTime);
        ADD_METRIC(rearrangeInitValuesCycles);
        ADD_METRIC(rearrangeInitValuesWorkers);
        ADD_METRIC(gatherPivotsElapsedTime);
        ADD_METRIC(gatherPivotsOutputBytes);
        ADD_METRIC(gatherPivotsInputBytes);
        ADD_METRIC(gatherPivotsMergeCycles);
        ADD_METRIC(gatherSuperPivotsElapsedTime);
        ADD_METRIC(gatherSuperPivotsOutputBytes);
        ADD_METRIC(gatherSuperPivotsInputBytes);
        ADD_METRIC(gatherSuperPivotsMergeCycles);
        ADD_METRIC(bcastPivotBucketBoundariesCycles);
        ADD_METRIC(bucketSortPivotsElapsedTime);
        ADD_METRIC(mergePivotsInBucketSortCycles);
        ADD_METRIC(allGatherPivotsElapsedTime);
        ADD_METRIC(allGatherPivotsMergeCycles);
        ADD_METRIC(shuffleKeysElapsedTime);
        ADD_METRIC(shuffleKeysInputBytes);
        ADD_METRIC(shuffleKeysOutputBytes);
        ADD_METRIC(shuffleKeysReceivedRpcs);
        ADD_METRIC(shuffleKeysSentRpcs);
        ADD_METRIC(shuffleValuesElapsedTime);
        ADD_METRIC(shuffleValuesInputBytes);
        ADD_METRIC(shuffleValuesOutputBytes);
        ADD_METRIC(shuffleValuesReceivedRpcs);
        ADD_METRIC(shuffleValuesSentRpcs);
        ADD_METRIC(bucketSortMergeKeyCycles);
        ADD_METRIC(bucketSortMergeValueCycles);
        ADD_METRIC(rearrangeFinalValuesElapsedTime);
        ADD_METRIC(rearrangeFinalValuesCycles);
        ADD_METRIC(rearrangeFinalValuesWorkers);
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
    foreach (double value, (*diff)[metric]) {
        result.append(format(formatString, value*scale));
    }
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
    for (size_t i = 0; i < v.size(); i++) {
        double value = 0;
        if (cycles[i] != 0) {
            value = v[i]/(cycles[i]/cyclesPerSecond[i]);
        }
        result.append(format(formatString, value*scale));
    }
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
    for (size_t i = 0; i < v1.size(); i++) {
        double value = (v2[i] == 0) ? 0 : v1[i]/v2[i];
        result.append(format(formatString, value*scale));
    }
    return result;
}
#pragma GCC diagnostic warning "-Wformat-nonliteral"

}  // namespace RAMCloud
