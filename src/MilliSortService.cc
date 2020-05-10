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

#include <emmintrin.h>
#include <immintrin.h>
#include <fstream>

#include "ips4o/ips4o.hpp"
#include "robin-hood/robin_hood.h"

#include "Arachne/DefaultCorePolicy.h"
#include "folly/AtomicUnorderedMap.h"
#include "BigQuery.h"
#include "ClockSynchronizer.h"
#include "MilliSortService.h"
#include "Broadcast.h"
#include "TimeTrace.h"
#include "Util.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

#define TIME_TRACE_SP 0

#if 0
/// Hack: redefine timeTrace to log so that we can use it even when server crashes.
#define timeTrace(format, ...) RAMCLOUD_LOG(NOTICE, format, ##__VA_ARGS__)
#define timeTraceSp(format, ...) RAMCLOUD_LOG(NOTICE, format, ##__VA_ARGS__)
#else

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

    inline void
    timeTraceSp(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE_SP
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

#endif

#define SCOPED_TIMER(metric) CycleCounter<> _timer_##metric(&PerfStats::threadStats.metric);
#define START_TIMER(metric) CycleCounter<> _timer_##metric(&PerfStats::threadStats.metric);
#define STOP_TIMER(metric) _timer_##metric.stop();

// TODO: the following won't work for network I/O bytes because that counter is accumulated in dispatch thread...
//#define START_COUNTER(metric) uint64_t _old_##metric = PerfStats::threadStats.metric;
//#define STOP_COUNTER(metric, counter) PerfStats::threadStats.counter += PerfStats::threadStats.metric - _old_##metric;

#define ADD_COUNTER(metric, delta) PerfStats::threadStats.metric += delta;
#define SUB_COUNTER(metric, delta) PerfStats::threadStats.metric -= delta;
#define INC_COUNTER(metric) PerfStats::threadStats.metric++;

// Change 0 -> 1 in the following line to use pseudo-random numbers to
// initialize the local keys.
#define USE_PSEUDO_RANDOM 1

// Change 0 -> 1 in the following line to overlap init. value rearrangement
// with the subsequent partition steps and key shuffle.
#define OVERLAP_INIT_VAL_REARRANGE 1

// Change 0 -> 1 in the following line to use SeoJin's online merge-sorter for
// sorting incoming keys; otherwise, ips4o as in local sort.
#define ENABLE_MERGE_SORTER 1

// Change 0 -> 1 in the following line to overlap merge-sorting incoming keys
// with record shuffle.
#define OVERLAP_MERGE_SORT 1

// Change 0 -> 1 in the following line to piggyback shuffle message chunks in
// the responses of shuffle RPCs to reduce # RPCs to send.
#define PIGGYBACK_SHUFFLE_MSG 1

// Change 0 -> 1 in the following line to enable two-level shuffle in the record
// shuffle step.
#define TWO_LEVEL_SHUFFLE 0

// Change 0 -> 1 in the following line to use the more sophisticated pivot
// selection algorithm which generates much more balanced data buckets.
#define GRADIENT_BASED_PIVOT_SELECTION 1

// Change 0 -> 1 in the following line to log intermediate computation result at
// each stage of the algorithm. Note: must be turned off during benchmarking.
#define LOG_INTERMEDIATE_RESULT 0

#define LOG_FINAL_RESULT 0

// Change 0 -> 1 to enable final result validation.
#define VALIDATE_RESULT 1

uint32_t MilliSortService::MAX_SHUFFLE_CHUNK_SIZE = 40000;

/**
 * Construct an MilliSortService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client. The new
 *      service will be registered in this context.
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 */
MilliSortService::MilliSortService(Context* context,
        const ServerConfig* serverConfig)
    : context(context)
    , block(2*1000*1000*1000)
    // FIXME: this memory region is read-only; you can't repurpose it this way!
//    , zeroCopyMemoryRegion(const_cast<void*>(context->masterZeroCopyRegion))
    , zeroCopyMemoryRegion(block.get())
    , communicationGroupTable()
    , ongoingMilliSort()
    , rand()
    , coresAvail()
    , startTime()
    , partitionStartTime()
    , pivotServerRank(-1)
    , isPivotServer()
    , numDataTuples(-1)
    , numPivotsPerServer(-1)
    , numPivotServers(-1)
    , localKeys((PivotKey*) zeroCopyMemoryRegion)
    , incomingKeys(localKeys + MAX_RECORDS_PER_NODE)
    , sortedKeys()
    , localValues((Value*) incomingKeys + MAX_RECORDS_PER_NODE)
    , localValues0(localValues + MAX_RECORDS_PER_NODE)
    , sortedValues(localValues0 + MAX_RECORDS_PER_NODE)
    , incomingValues(sortedValues + MAX_RECORDS_PER_NODE)
    , numSortedItems(0)
    , numRecordsReceived()
    , printingResult()
    , shuffleRecordResv()
    , localPivots()
    , rearrangeLocalVals()
    , splitters()
    , dataBucketRanges()
    , recordShuffleRxCount()
    , recordShuffleProgress()
    , recordShuffleMessages()
    , recordShuffleIsReady()
    , rowShuffleMessages()
    , colShuffleMessages()
    , colShuffleMessageLocks()
    , rowShuffleIsReady()
    , colShuffleIsReady()
    , rowShuffleRxCount()
    , colShuffleRxCount()
    , rowShuffleProgress()
    , colShuffleProgress()
    , globalShuffleOpTable()
    , mergeSorterLock("MilliSortService::mergeSorter")
    , mergeSortChunks()
    , mergeSortTmpBuffer()
    , world()
    , twoLevelShuffleRowGroup()
    , twoLevelShuffleColGroup()
    , sessions()
    , myPivotServerGroup()
    , gatheredPivots()
    , superPivots()
    , pivotBucketBoundaries()
    , sortedGatheredPivots()
    , numActiveSources()
    , numNonBeginPivotsPassed()
    , numPivotsPassed()
    , allPivotServers()
    , broadcastSplitters()
    , pivotShuffleIsReady()
    , pivotShuffleProgress()
    , pivotShuffleRxCount()
    , pivotShuffleMessages()
    , globalSuperPivots()
    , bcastPivotBucketBoundaries()
    // BigQuery-related
    , bigQueryQ4UniqueWords()
    , bigQueryQ4BcastDone()
{
    context->services[WireFormat::MILLISORT_SERVICE] = this;
    if (uint64_t(localKeys) % 64) {
        DIE("localKeys not aligned to cache line!");
    }
    if (uint64_t(localValues) % 64) {
        DIE("localValues not aligned to cache line!");
    }
    if (uint64_t(localValues0) % 64) {
        DIE("localValues0 not aligned to cache line!");
    }
    if (uint64_t(incomingKeys) % 64) {
        DIE("incomingKeys not aligned to cache line!");
    }
    if (uint64_t(sortedValues) % 64) {
        DIE("sortedalues not aligned to cache line!");
    }
    if (uint64_t(incomingValues) % 64) {
        DIE("incomingValues not aligned to cache line!");
    }
    if (uint64_t(incomingValues + MAX_RECORDS_PER_NODE) >
            uint64_t(block.get()) + block.length) {
        DIE("incomingValues outside the range of allocated memory!");
    }

    char* str = std::getenv("MILLISORT_SHUFFLE_CHUNK_SIZE");
    if (str) {
        std::string val(str);
        MAX_SHUFFLE_CHUNK_SIZE = std::stoul(val);
    }
    LOG(NOTICE, "MAX_SHUFFLE_CHUNK_SIZE = %u", MAX_SHUFFLE_CHUNK_SIZE);
}

MilliSortService::~MilliSortService()
{
    context->services[WireFormat::MILLISORT_SERVICE] = NULL;
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
MilliSortService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::InitMilliSort::opcode:
            callHandler<WireFormat::InitMilliSort, MilliSortService,
                        &MilliSortService::initMilliSort>(rpc);
            break;
        case WireFormat::StartMilliSort::opcode:
            callHandler<WireFormat::StartMilliSort, MilliSortService,
                        &MilliSortService::startMilliSort>(rpc);
            break;
        case WireFormat::TreeBcast::opcode:
            callHandler<WireFormat::TreeBcast, MilliSortService,
                        &MilliSortService::treeBcast>(rpc);
            break;
        case WireFormat::TreeGather::opcode:
            callHandler<WireFormat::TreeGather, MilliSortService,
                        &MilliSortService::treeGather>(rpc);
            break;
        case WireFormat::AllGather::opcode:
            callHandler<WireFormat::AllGather, MilliSortService,
                        &MilliSortService::allGather>(rpc);
            break;
        case WireFormat::SendData::opcode:
            callHandler<WireFormat::SendData, MilliSortService,
                        &MilliSortService::sendData>(rpc);
            break;
        case WireFormat::ShufflePush::opcode:
            callHandler<WireFormat::ShufflePush, MilliSortService,
                        &MilliSortService::shufflePush>(rpc);
            break;
        case WireFormat::BenchmarkCollectiveOp::opcode:
            callHandler<WireFormat::BenchmarkCollectiveOp, MilliSortService,
                        &MilliSortService::benchmarkCollectiveOp>(rpc);
            break;
        default:
            prepareErrorResponse(rpc->replyPayload,
                    STATUS_UNIMPLEMENTED_REQUEST);
    }
}


/**
 * Top-level server method to handle the INIT_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::initMilliSort(const WireFormat::InitMilliSort::Request* reqHdr,
        WireFormat::InitMilliSort::Response* respHdr, Rpc* rpc)
{
    timeTrace("initMilliSort invoked, id %u, dataTuplesPerServer %u, "
              "pivotsPerServer %u, nodesPerPivotServer %u", reqHdr->id,
              reqHdr->dataTuplesPerServer, reqHdr->pivotsPerServer,
              reqHdr->nodesPerPivotServer);

    // Mutex used to serialize calls of this method.
    static Arachne::SpinLock mutex("MilliSortService::initMilliSort");
    static uint32_t lastInitRpcId;
    static WireFormat::InitMilliSort::Response lastInitRpcResp;

    std::lock_guard<Arachne::SpinLock> _(mutex);

    // Check if we have just finished processing the same init. RPC; if so,
    // just return the same response stored earlier.
    bool duplicateInitReq = (reqHdr->id == lastInitRpcId);
    if (duplicateInitReq) {
        LOG(WARNING, "Skipped duplicate InitMilliSort RPC (id = %u)",
                reqHdr->id);
        *respHdr = lastInitRpcResp;
        return;
    } else {
        lastInitRpcId = reqHdr->id;
        lastInitRpcResp = {};
    }

    // We are busy serving another sorting request.
    if (ongoingMilliSort) {
        LOG(WARNING, "Sorting already in progress!");
        throw RetryException(HERE, 250*1000, 500*1000,
                "Sorting already in progress");
        return;
    }

    // Initialize the world communication group so we know the rank of this
    // server.
    initWorld(reqHdr->numNodes);
    sessions.clear();
    for (int i = 0; i < world->size(); i++) {
        sessions.push_back(context->serverList->getSession(world->getNode(i)));
    }

    // Initialization request from an external client should only be processed
    // by the root node, which also performs error checking.
    if (reqHdr->fromClient) {
        if ((world->rank > 0) ||
                (numDataTuples * MAX_IMBALANCE_RATIO > MAX_RECORDS_PER_NODE)) {
            LOG(ERROR, "Illegal arguments: rank %d, numDataTuples %d",
                    world->rank, numDataTuples);
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }
    }

    // Initiate the init. RPCs to its children nodes in the world group;
    // unfortunately, we can't (re-)use the broadcast mechanism because the
    // world group has not been setup on the target nodes.
    std::list<InitMilliSortRpc> outgoingRpcs;
    const int fanout = 10;
    for (int i = 0; i < fanout; i++) {
        int rank = world->rank * fanout + i + 1;
        if (rank >= world->size()) {
            break;
        }
        outgoingRpcs.emplace_back(context, world->getNode(rank),
                reqHdr->numNodes, reqHdr->dataTuplesPerServer,
                reqHdr->pivotsPerServer, reqHdr->nodesPerPivotServer,
                reqHdr->flushCache, false);
    }

    // Initialize member fields.
    startTime = 0;
    numDataTuples = (int) reqHdr->dataTuplesPerServer;
    numPivotsPerServer = (int) reqHdr->pivotsPerServer;
    int nodesPerPivotServer = (int) reqHdr->nodesPerPivotServer;
    pivotServerRank = world->rank / nodesPerPivotServer *
            nodesPerPivotServer;
    isPivotServer = (pivotServerRank == world->rank);
    numPivotServers = world->size() / nodesPerPivotServer +
            (world->size() % nodesPerPivotServer > 0 ? 1 : 0);

    // Construct communication sub-groups.
    communicationGroupTable.clear();
    registerCommunicationGroup(world.get());
    vector<ServerId> nodes;
    for (int i = 0; i < nodesPerPivotServer; i++) {
        if (pivotServerRank + i >= world->size()) {
            break;
        }
        nodes.push_back(world->getNode(pivotServerRank + i));
    }
    myPivotServerGroup.construct(MY_PIVOT_SERVER_GROUP,
            world->rank % nodesPerPivotServer, nodes);
    registerCommunicationGroup(myPivotServerGroup.get());
    if (isPivotServer) {
        nodes.clear();
        for (int i = 0; i < numPivotServers; i++) {
            nodes.push_back(world->getNode(i * nodesPerPivotServer));
        }
        allPivotServers.construct(ALL_PIVOT_SERVERS,
                world->rank / nodesPerPivotServer, nodes);
        registerCommunicationGroup(allPivotServers.get());
    } else {
        allPivotServers.destroy();
    }

#if TWO_LEVEL_SHUFFLE
    // TODO: compute ncols and nrows without using FP arithmetic
    int ncols = downCast<int>(ceil(sqrt(world->size())));
    int nrows = downCast<int>(ceil(world->size() / ncols));
    if (ncols * nrows != world->size()) {
        DIE("Unsupported # servers? ncols %d, nrows %d", ncols, nrows);
    }
    int rowId = world->rank / ncols;
    int colId = world->rank % ncols;
    nodes.clear();
    for (int i = rowId * ncols; i < (rowId + 1) * ncols; i++) {
        if (i >= world->size()) {
            break;
        }
        nodes.push_back(world->getNode(i));
    }
    twoLevelShuffleRowGroup.construct(TWO_LEVEL_ROW_SERVERS, colId, nodes);

    nodes.clear();
    for (int i = colId; i < world->size(); i += ncols) {
        nodes.push_back(world->getNode(i));
    }
    twoLevelShuffleColGroup.construct(TWO_LEVEL_COL_SERVERS, rowId, nodes);
#endif

    coresAvail.clear();
    auto coreList = Arachne::getCorePolicy()->getCores(0);
    for (uint32_t i = 0; i < coreList.size(); i++) {
        coresAvail.push_back(coreList[i]);
    }
    std::sort(coresAvail.begin(), coresAvail.end());

    // Generate data tuples.
    rand.construct(serverId.indexNumber());
    for (int i = 0; i < numDataTuples; i++) {
#if USE_PSEUDO_RANDOM
        uint64_t key = rand->next();
#else
        uint64_t key = uint64_t(world->rank + i * world->size());
#endif
        new(&localKeys[i]) PivotKey(key,
                downCast<uint16_t>(serverId.indexNumber() - 1),
                downCast<uint32_t>(i));
        new(&localValues0[i]) Value(key);
    }
//    cacheflush(localKeys, numDataTuples * PivotKey::SIZE);
//    cacheflush(localValues0, numDataTuples * Value::SIZE);

    while (printingResult) {
        Arachne::yield();
    }
    sortedKeys = NULL;
    numSortedItems = 0;
    numRecordsReceived = 0;
    shuffleRecordResv.construct(world->size());
    for (int i = 0; i < world->size(); i++) {
        shuffleRecordResv->at(i).store(-1);
    }

    //
    localPivots.clear();
    rearrangeLocalVals.construct(localKeys, localValues0, localValues);
    splitters.clear();
    dataBucketRanges.clear();
    recordShuffleRxCount = 0;
    recordShuffleProgress.construct(world->size());
    recordShuffleMessages.construct(world->size());
    recordShuffleIsReady = false;

#if TWO_LEVEL_SHUFFLE
    rowShuffleMessages.construct(twoLevelShuffleRowGroup->size());
    colShuffleMessages.construct(twoLevelShuffleColGroup->size());
    colShuffleMessageLocks.construct(twoLevelShuffleColGroup->size());
    rowShuffleIsReady = false;
    colShuffleIsReady = false;
    rowShuffleRxCount = 0;
    colShuffleRxCount = 0;
    rowShuffleProgress.construct(twoLevelShuffleRowGroup->size());
    colShuffleProgress.construct(twoLevelShuffleColGroup->size());
#endif

    mergeSortChunks.clear();
    // Release the old buffer first before `new`, hoping to reuse the same
    // memory.
    mergeSortTmpBuffer.reset(NULL);
    mergeSortTmpBuffer.reset(new PivotKey[numDataTuples*2]);
    gatheredPivots.clear();
    superPivots.clear();
    pivotBucketBoundaries.clear();
    sortedGatheredPivots.clear();
    numActiveSources = 0;
    numNonBeginPivotsPassed = 0;
    numPivotsPassed = 0;
    if (isPivotServer) {
        broadcastSplitters.construct(context, myPivotServerGroup.get());
        pivotShuffleIsReady = false;
        pivotShuffleRxCount = 0;
        pivotShuffleProgress.construct(allPivotServers->size());
        pivotShuffleMessages.construct(allPivotServers->size());
    }
    globalSuperPivots.clear();
    if (world->rank == 0) {
        bcastPivotBucketBoundaries.construct(context, allPivotServers.get());
    }

    // Clean up shuffle operations.
    for (auto& entry : globalShuffleOpTable) {
        ShuffleOp* shuffleOp = entry.exchange(NULL);
        delete shuffleOp;
    }

    // Clear BigQuery-related states
    bigQueryQ4UniqueWords.clear();
    bigQueryQ4BcastDone = false;

    // Flush LLC to avoid generating unrealistic numbers; we are doing
    // memory-to-memory sorting (not cache-to-cache sorting)!
    if (reqHdr->flushCache) {
        // FIXME: skip cache flushing to speed up running many millisort
        // experiments with vary config. parameters.
//        flushSharedCache();
//        timeTrace("initMilliSort: flushed shared cache; ready for end-to-end "
//                "benchmark");
    }

    // Wait for its children nodes to finish initialization.
    while (!outgoingRpcs.empty()) {
        InitMilliSortRpc* initRpc = &outgoingRpcs.front();
        if (initRpc->isReady()) {
            auto initResult = initRpc->wait();
            respHdr->numNodesInited += initResult->numNodesInited;
            outgoingRpcs.pop_front();
        }
        Arachne::yield();
    }

    // Fill out the RPC response.
    respHdr->numNodesInited += 1;
    if (reqHdr->fromClient) {
        respHdr->numNodes = respHdr->numNodesInited;
        respHdr->numCoresPerNode = static_cast<int>(
                Arachne::numActiveCores.load());
        respHdr->keySize = PivotKey::KEY_SIZE;
        respHdr->valueSize = Value::SIZE;
    }
    lastInitRpcResp = *respHdr;
    timeTrace("initMilliSort: rank %d initialized %d millisort nodes",
            world->rank, respHdr->numNodesInited);
}

/**
 * Top-level server method to handle the START_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::startMilliSort(const WireFormat::StartMilliSort::Request* reqHdr,
        WireFormat::StartMilliSort::Response* respHdr, Rpc* rpc)
{
    timeTrace("startMilliSort invoked, requestId %u, fromClient %u",
            uint(reqHdr->requestId), reqHdr->fromClient);

    // Mutex used to serialize calls of this method.
    static Arachne::SpinLock mutex("MilliSortService::startMilliSort");
    static int32_t lastMilliSortId = -1;

    // Start request from external client should only be processed by the root
    // node, which would then broadcast the start request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            LOG(ERROR, "Illegal arguments: rank %d", world->rank);
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        if (mutex.try_lock()) {
            if (reqHdr->requestId == lastMilliSortId) {
                LOG(WARNING, "Skipped duplicate StartMilliSort RPC from client,"
                        " requestId = %d", reqHdr->requestId);
                mutex.unlock();
                return;
            }
            mutex.unlock();
        } else {
            // Can't acquire the lock: there must another millisort in progress;
            // retry later.
            uint64_t delayMicros = 250 * 1000;
            throw RetryException(HERE, delayMicros, delayMicros * 2,
                    "Another millisort in progress");
        }

        // Set the global time where all nodes should start millisort.
        startTime = Cycles::rdtsc() + Cycles::fromMicroseconds(200);

        // Start broadcasting the start request.
        TreeBcast startBcast(context, world.get());
        startBcast.send<StartMilliSortRpc>(reqHdr->requestId, reqHdr->queryNo,
                startTime, false);
        startBcast.wait();
        timeTrace("MilliSort finished at master node, requestId %d",
                reqHdr->requestId);
        return;
    }

    // Acquire the lock to make sure no ongoing millisort is running on this
    // server. Skip millisort if we have received a duplicate RPC request.
    std::lock_guard<Arachne::SpinLock> _(mutex);
    bool duplicateRequest = (reqHdr->requestId == lastMilliSortId);
    if (duplicateRequest) {
        LOG(WARNING, "Skipped duplicate StartMilliSort RPC (requestId = %d)",
                reqHdr->requestId);
        return;
    } else {
        lastMilliSortId = reqHdr->requestId;
    }

    // Wait until the designated start time set by the root node.
    TimeConverter converter =
            context->clockSynchronizer->getConverter(world->getNode(0));
    startTime = converter.isValid() ?
            converter.toLocalTsc(reqHdr->startTime) : Cycles::rdtsc();
    if (Cycles::rdtsc() > Cycles::fromMicroseconds(1) + startTime) {
        LOG(ERROR, "Millisort started %.2f us late!",
                Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
    }
    while (Cycles::rdtsc() < startTime) {
        Arachne::yield();
    }

    // Kick start the sorting process.
    ADD_COUNTER(millisortNodes, world->size());
    ADD_COUNTER(millisortIsPivotSorter, isPivotServer ? 1 : 0);

    if (reqHdr->queryNo == 0) {
        timeTrace("MilliSort algorithm started, run %d", reqHdr->requestId);
        ongoingMilliSort = rpc;
        localSortAndPickPivots();
        // FIXME: a better solution is to wait on condition var.
        while (ongoingMilliSort != NULL) {
            Arachne::yield();
        }
        timeTrace("MilliSort algorithm run %d completed", reqHdr->requestId);
    } else {
        timeTrace("BigQuery Q%d started, run %d", reqHdr->queryNo,
                reqHdr->requestId);
        switch (reqHdr->queryNo) {
            case 1: bigQueryQ1(); break;
            case 2: bigQueryQ2(); break;
            case 3: bigQueryQ3(); break;
            case 4: bigQueryQ4(); break;
            default:
               DIE("Unexpected queryNo %d", reqHdr->queryNo);
        }
        timeTrace("BigQuery Q%d run %d completed", reqHdr->queryNo,
                reqHdr->requestId);
    }
}

void
MilliSortService::moveValues(PivotKey* keys, Value* oldValues, Value* newValues,
        int start, int numRecords)
{
#if __AVX2__
if constexpr (Value::SIZE == 90) {
    // In general, when the value size is not a multiple of 16/32/64 and we
    // want to use aligned NT stores, we need to first manually accumulate
    // source values into cacheline-sized chunks so that we can write data
    // one cache line at a time.

    // Allocate an array of 32-byte integers (__m256i means 256-bit integer)
    // large enough to hold one full value plus one 32-byte integer. Thus,
    // we need 4*32B to hold a 90-byte value plus a 32-byte integer.
    __m256i tmp[5];
    // Tail position where dest values should be appended to.
    char* valueDst = (char*) &newValues[start];
    // How many bytes of the first value should be used to pad
    // `valueDst` to a 32-byte boundary?
    uint32_t bytesToPad = (32 - uint64_t(valueDst) % 32) % 32;
    while (bytesToPad + 32 <= Value::SIZE) {
        bytesToPad += 32;
    }
    char* firstVal = (char*) (oldValues + keys[start].index);
    memcpy(valueDst, firstVal, bytesToPad);
    valueDst += bytesToPad;

    // Copy the rest of the first value into the tmp buffer.
    memcpy(tmp, firstVal + bytesToPad, Value::SIZE - bytesToPad);
    // # bytes accumulated in tmp.
    uint32_t tmpSize = Value::SIZE - bytesToPad;

    // Start copying from the second value.
    for (int i = start + 1; i < start + numRecords; i++) {
        uint32_t index = keys[i].index;
        __m256i* valueSrc = (__m256i*) &oldValues[index];
        __m256i* tmpEnd = (__m256i*)((char*)tmp + tmpSize);
        // Note: unfortunately, in order to accumulate/grow data in the tmp
        // buffer, we have to use unaligned stores (this was not necessary in
        // the 64B value case where we can just load data into xmm registers).
        _mm256_storeu_si256(tmpEnd+0, _mm256_loadu_si256(valueSrc+0));
        _mm256_storeu_si256(tmpEnd+1, _mm256_loadu_si256(valueSrc+1));
        _mm256_storeu_si256(tmpEnd+2, _mm256_loadu_si256(valueSrc+2));
        tmpSize += Value::SIZE;
        _mm256_stream_si256((__m256i*)valueDst, tmp[0]);
        _mm256_stream_si256((__m256i*)valueDst + 1, tmp[1]);
        tmpSize -= 64;
        if (tmpSize >= 32) {
            tmpSize -= 32;
            _mm256_stream_si256((__m256i*)valueDst + 2, tmp[2]);
            valueDst += 96;
            tmp[0] = tmp[3];
        } else {
            valueDst += 64;
            tmp[0] = tmp[2];
        }
    }

    // Clear out any remaining bytes in the tmp buffer.
    memcpy(valueDst, (char*)tmp + tmpSize, tmpSize);
} else if constexpr (Value::SIZE == 96) {
    __m256i* valueDst = (__m256i*) &newValues[start];
    for (int i = start; i < start + numRecords; i++) {
        uint32_t index = keys[i].index;
        __m256i* src = (__m256i*) (oldValues + index);
        __m256i ymm1 = _mm256_stream_load_si256(src);
        __m256i ymm2 = _mm256_stream_load_si256(src + 1);
        __m256i ymm3 = _mm256_stream_load_si256(src + 2);
        _mm256_stream_si256(valueDst++, ymm1);
        _mm256_stream_si256(valueDst++, ymm2);
        _mm256_stream_si256(valueDst++, ymm3);
    }
} else {
    for (int i = start; i < start + numRecords; i++) {
        uint32_t index = keys[i].index;
        memcpy(newValues + i, oldValues + index, Value::SIZE);
    }
}
#else
    for (int i = start; i < start + numRecords; i++) {
        uint32_t index = keys[i].index;
        memcpy(newValues + i, oldValues + index, Value::SIZE);
    }
#endif
}

/**
 * FIXME: add docs
 * \param keys
 * \param numKeys
 * \param numPartitions
 * \param includeHead
 * \param pivots
 */
void
MilliSortService::partition(PivotKey* keys, int numKeys, int numPartitions,
        bool includeHead, std::vector<PivotKey>* pivots)
{
    assert(pivots->empty());
    if (includeHead) {
        pivots->push_back(keys[0]);
    }

    // Let P be the number of partitions. Pick P pivots that divide #keys into
    // P partitions as evenly as possible. Since the sizes of any two partitions
    // differ by at most one, we have:
    //      s * k + (s + 1) * (P - k) = numKeys
    // where s is the number of keys in smaller partitions and k is the number
    // of smaller partitions.
    int s = numKeys / numPartitions;
    int k = numPartitions * (s + 1) - numKeys;
    int pivotIdx = -1;
    for (int i = 0; i < numPartitions; i++) {
        if (i < k) {
            pivotIdx += s;
        } else {
            pivotIdx += s + 1;
        }
        pivots->push_back(keys[pivotIdx]);
    }
}

/**
 *
 *
 * \param useCurrentCore
 *      True means we are allowed to spawn Arachne threads on the core that
 *      runs the current thread; false, otherwise. Only set to true if there
 *      is no latency-sensitive task to perform.
 */
void
MilliSortService::rearrangeValues(RearrangeValueTask* task, int totalItems,
        bool initialData)
{
//    LOG(NOTICE, "keys at %p (seq. read), src. values at %p (random read) "
//            "dest. values at %p (seq. write)", keys, src, dest);
    task->startTime = Cycles::rdtsc();
    uint64_t* numWorkers;
    if (initialData) {
        ADD_COUNTER(rearrangeInitValuesStartTime, task->startTime - startTime);
        numWorkers = &PerfStats::threadStats.rearrangeInitValuesWorkers;
    } else {
        ADD_COUNTER(rearrangeFinalValuesStartTime, task->startTime - startTime);
        numWorkers = &PerfStats::threadStats.rearrangeFinalValuesWorkers;
    }

    /**
     * Main method of the worker thread. Each worker is responsible to rearrange
     * values corresponding to a partition of the keys.
     *
     * For instance, if this worker is assigned keys[a..b], this method will
     * ensure that, forall i (a <= i <= b), values[i] holds the corresponding
     * value of keys[i] upon return.
     *
     * \param startIdx
     *      Index of the first key in this worker's key partition.
     * \param n
     *      # values assigned to this worker. So this worker should rearrange
     *      values corresponding to keys[startIdx...(startIdx+numValues-1)].
     */
    auto workerMain = [task, totalItems, initialData]
    {
        // Unpack arguments.
        PivotKey* keys = task->keys;
        Value* src = task->src;
        Value* dest = task->dst;

        timeTrace("RearrangeValue worker started, coreId %d", Arachne::core.id);
        uint64_t* cpuTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesCycles :
                &PerfStats::threadStats.rearrangeFinalValuesCycles;

        // FIXME: how should I choose this number? too large => load imbalance
        // (not all HTs run equally fast: hypertwin of dispatch thread and the
        // hypertwin of the non-Arachne core runs faster); too small => cache
        // thrashing between HTs?? messing up with memory prefetcher?
        const int maxItemsPerTask = 128;
        int workDone = 0;
        uint64_t busyTime = 0;
        while (true) {
            uint64_t now = Cycles::rdtsc();
            int startIdx = task->numSortedValues->fetch_add(maxItemsPerTask);
            int numItems = std::min(maxItemsPerTask, totalItems - startIdx);
            if (numItems <= 0) {
                break;
            }

            // Note: it's OK to leave the stale value in keys[i].index because
            // after rearrangement, the correspondence between keys and values
            // is trivial (ie. keys[i] -> values[i]).
            moveValues(keys, src, dest, startIdx, numItems);
            workDone += numItems;
            busyTime += (Cycles::rdtsc() - now);
            Arachne::yield();
        }
        task->workerDone(Cycles::rdtsc());
        timeTrace("Worker rearranged %d values, busy %lu us, cpu %d, "
                "initialData %u", workDone, Cycles::toMicroseconds(busyTime),
                Arachne::core.id, initialData);
        (*cpuTime) += busyTime;
    };

    // Spin up worker threads on all cores we are allowed to use.
    timeTrace("Spawning workers on coreId %d", Arachne::core.id);
    for (int coreId : coresAvail) {
        if (initialData && (coreId == Arachne::core.id)) {
            continue;
        }
        task->workers.push_back(
                Arachne::createThreadOnCore(coreId, workerMain));
        (*numWorkers)++;
    }
}

void
MilliSortService::bigQueryQ1()
{
    /// Hashmap keeping the number of views of each language.
//    using GroupByMap = std::unordered_map<std::string, uint64_t>;
    using GroupByMap = robin_hood::unordered_map<std::string, uint64_t>;

    /// Columnar storage for the input table.
    using LangCol = std::vector<std::string>;
    using ViewCol = std::vector<uint64_t>;

    /// Group records of (lang, views)-pairs by the first element; sum up the
    /// views.
    struct GroupByLang : public TreeGather::Merger {
        /// Aggregate partial results from children nodes.
        GroupByMap* numViews;

        /// Serializes #numViews in wire format so it can be sent over the
        /// network.
        Buffer result;

        /// Protects #numViews.
        Arachne::SpinLock mutex;

        explicit GroupByLang(GroupByMap* numViews)
            : numViews(numViews), result(), mutex() {}

        /**
         * Update the aggregation result with the partial result coming from
         * a children node.
         */
        void add(Buffer* partialResult)
        {
            uint32_t length = partialResult->size();
            timeTrace("GroupByLang::add invoked, buffer size %u", length);
            size_t cnt = 0;
            static const size_t MIN_BATCH = 8;
            static const size_t MAX_BATCH = 32;
            std::array<std::pair<std::string, uint64_t>, MAX_BATCH> tmp;
            uint32_t offset = 0;
            while (offset < length) {
                tmp[cnt].first.clear();
                while (char c = *partialResult->read<char>(&offset)) {
                    tmp[cnt].first.append(1, c);
                }
                tmp[cnt].second = *partialResult->read<uint64_t>(&offset);

                cnt = (cnt + 1) % MAX_BATCH;
                // Check if we can drain the temp buffer once in a while.
                if (cnt % MIN_BATCH == 0) {
                    if (!mutex.try_lock()) {
                        // If the buffer is full, we must acquire the lock and
                        // drain it; otherwise, keep on parsing more KV pairs.
                        if (cnt % MAX_BATCH != 0) {
                            continue;
                        }
                        timeTrace("GroupByLang::add contending for lock");
                        mutex.lock();
                    }
                    timeTrace("GroupByLang::add acquired lock");
                    for (size_t i = 0; i < cnt; i++) {
                        (*numViews)[tmp[i].first] += tmp[i].second;
                    }
                    cnt = 0;
                    mutex.unlock();
                    timeTrace("GroupByLang::add released lock");
                }
            }

            std::lock_guard<Arachne::SpinLock> lock(mutex);
            for (size_t i = 0; i < cnt; i++) {
                (*numViews)[tmp[i].first] += tmp[i].second;
            }
            timeTrace("GroupByLang::add finished");
        }

        Buffer* getResult()
        {
            if (result.size() == 0) {
                timeTrace("GroupByLang::getResult serializing current result");
                for (auto& kv : *numViews) {
                    result.appendCopy(kv.first.c_str(), kv.first.size() + 1);
                    result.appendCopy(&kv.second);
                }
                timeTrace("GroupByLang::getResult finished serialization");
            }
            return &result;
        }
    };

    // Step 0: initialize input data
    static const int recordsPerTablet = 10*1000000;
    static std::pair<LangCol, ViewCol> tablet;
    uint64_t initTime = 0;
    if (tablet.first.size() != numDataTuples) {
        CycleCounter<> _(&initTime);
        LangCol& langCol = tablet.first;
        ViewCol& viewCol = tablet.second;
        langCol.clear();
        langCol.reserve(numDataTuples);
        viewCol.clear();
        viewCol.reserve(numDataTuples);
        int sid = serverId.indexNumber() - 1;
        int startRecId = sid * numDataTuples;
        int stopRecId = (sid + 1) * numDataTuples - 1;
        size_t totalRecords = 0;
        size_t totalKeyBytes = 0;
        for (int tabletId = startRecId / recordsPerTablet;
                tabletId <= stopRecId / recordsPerTablet; tabletId++) {
            int curRecId = tabletId * recordsPerTablet - 1;
            string filename = format(
                    "/home/yilongl/work/wiki1B/tablet-%03d.txt", tabletId);
            std::ifstream tabletFile(filename.c_str());
            if (tabletFile.is_open()) {
                std::string line;
                while (std::getline(tabletFile, line)) {
                    curRecId++;
                    if (curRecId < startRecId) continue;
                    if (curRecId > stopRecId) break;
                    size_t comma = line.find(',');
                    if (comma == string::npos) continue;
                    std::string lang = line.substr(0, comma);
                    uint64_t views = std::stol(line.substr(comma + 1));
                    langCol.emplace_back(lang);
                    viewCol.emplace_back(views);
                    totalKeyBytes += lang.size();
                }
            } else {
                LOG(ERROR, "File not found: %s", filename.c_str());
            }
            LOG(NOTICE, "Initialized tablet with %lu records from %s",
                    langCol.size() - totalRecords, filename.c_str());
            totalRecords = langCol.size();
        }
        LOG(NOTICE, "Average key length %.2f bytes",
                double(totalKeyBytes) / double(totalRecords));
    }
    timeTrace("BigQuery Q1 started");

    // Step 1: scan the local tablet to obtain the partial result; use all
    // worker cores to parallelize the work.
    std::vector<GroupByMap> groupByMaps(coresAvail.size());
    std::atomic<size_t> nextStart(0);

    /// Scanner function that processes a number of (lang, views) pairs and
    /// count the views per language.
    auto scanner = [&nextStart] (std::pair<LangCol, ViewCol>* tablet,
            GroupByMap* groupBy) {
        timeTrace("worker started on core %d", Arachne::core.id);
        static const int BATCH_SIZE = 512;

        LangCol& langCol = tablet->first;
        ViewCol& viewCol = tablet->second;
        size_t numRecords = 0;
        while (true) {
            size_t start = nextStart.fetch_add(BATCH_SIZE);
            size_t end = std::min(start + BATCH_SIZE, langCol.size());
            if (start >= langCol.size()) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                (*groupBy)[langCol[i]] += viewCol[i];
            }
            numRecords += (end - start);
        }
        timeTrace("worker done, processed %u records", numRecords);
    };

    auto combiner = [] (GroupByMap* a, GroupByMap* b) {
        for (auto& kv : *b) {
            (*a)[kv.first] += kv.second;
        }
    };

    {
        SCOPED_TIMER(bigQueryStep1ElapsedTime)
        std::vector<Arachne::ThreadId> workers;
        int id = 0;
        for (int coreId : coresAvail) {
            workers.push_back(Arachne::createThreadOnCore(coreId, scanner,
                    &tablet, &groupByMaps[id]));
            id++;
        }
        for (auto& tid : workers) {
            Arachne::join(tid);
        }
        workers.clear();
        timeTrace("step 1: scanned the table to sum up views per language");

        size_t n = groupByMaps.size();
        while (n > 1) {
            size_t k = (n / 2);
            size_t s = (n % 2);
            for (size_t i = 0; i < k; i++, s++) {
                workers.push_back(Arachne::createThreadOnCore(coresAvail[i],
                        combiner, &groupByMaps[s], &groupByMaps[s + k]));
            }
            n -= k;

            for (auto& tid : workers) {
                Arachne::join(tid);
            }
            workers.clear();
        }
        groupByMaps.resize(1);
        timeTrace("step 1: combined per-core results, languages %u",
                groupByMaps.front().size());
    }

    // Step 2: reduce/aggregate partial results back to the root
    const int DUMMY_OP_ID = 999;
    GroupByLang countViewsByLang(&groupByMaps.front());
    {
        SCOPED_TIMER(bigQueryStep2ElapsedTime)
        Tub<TreeGather> gatherOp;
        invokeCollectiveOp<TreeGather>(gatherOp, DUMMY_OP_ID, context,
                world.get(), 0, &countViewsByLang);
        gatherOp->wait();
        removeCollectiveOp(DUMMY_OP_ID);
        timeTrace("step 2: gather op completed");
    }
    uint64_t elapsed = Cycles::rdtsc() - startTime - initTime;
    ADD_COUNTER(bigQueryTime, elapsed);

    // Pretty-print the query result that will be returned to the BQ client
    if (world->rank == 0) {
        std::vector<std::pair<std::string, uint64_t>> result;
        for (auto& kv : groupByMaps.front()) {
            result.emplace_back(kv.first, kv.second);
        }
        std::sort(result.begin(), result.end());
        std::string prettyPrint;
        for (size_t row = 0; row < result.size(); row++) {
            prettyPrint += format("%lu, %s, %lu | ", row + 1,
                    result[row].first.c_str(), result[row].second);
        }
        LOG(NOTICE, "BigQuery Q1 result (scan %d records in %.3f ms):\n%s",
                numDataTuples * world->size(), Cycles::toSeconds(elapsed)*1e3,
                prettyPrint.c_str());
    } else {
        LOG(NOTICE, "Finished processing BigQuery Q1");
    }
}

void
MilliSortService::bigQueryQ2() {
    using Ipv4Address = uint32_t;
    using IPv4Col = std::vector<Ipv4Address>;

    /// # top IPv4 addresses to keep, specified in the LIMIT clause.
    static const size_t LIMIT_K = 10;

    // Step 0: initialize input data
    static IPv4Col tablet;
    static const int recordsPerTablet = 10*1000000;
    uint64_t initTime = 0;
    if (tablet.size() != numDataTuples) {
        CycleCounter<> _(&initTime);
        tablet.clear();
        tablet.reserve(numDataTuples);
        int sid = serverId.indexNumber() - 1;
        int startRecId = sid * numDataTuples;
        int stopRecId = (sid + 1) * numDataTuples - 1;
        size_t totalRecords = 0;
        for (int tabletId = startRecId / recordsPerTablet;
                tabletId <= stopRecId / recordsPerTablet; tabletId++) {
            int curRecId = tabletId * recordsPerTablet - 1;
            string filename = format(
                    "/home/yilongl/work/wiki_ip/tablet-%03d.csv", tabletId);
            std::ifstream tabletFile(filename.c_str());
            if (tabletFile.is_open()) {
                std::string line;
                std::string token;
                while (std::getline(tabletFile, line)) {
                    curRecId++;
                    if (curRecId < startRecId) continue;
                    if (curRecId > stopRecId) break;
                    try {
                        Ipv4Address ip = 0;
                        std::istringstream stream(line);
                        while (std::getline(stream, token, '.')) {
                            ip <<= 8;
                            ip |= token.empty() ? 0 :
                                    uint32_t(std::stoi(token));
                        }
                        tablet.emplace_back(ip);
                    } catch (...) {
                        LOG(WARNING, "Couldn't parse address %s", line.c_str());
                    }
                }
            } else {
                LOG(ERROR, "File not found: %s", filename.c_str());
            }
            LOG(NOTICE, "Initialized tablet with %d records from %s",
                    tablet.size() - totalRecords, filename.c_str());
            totalRecords = tablet.size();
        }
    }

    // Step 1: scan the local tablet to build the shuffle messages; use all
    // worker cores to parallelize the work.
    using PartitionByDest = std::vector<Buffer>;
    std::vector<Tub<PartitionByDest>> msgsPerCore(coresAvail.size());
    for (auto &tub : msgsPerCore) {
        tub.construct(world->size());
    }
    std::atomic<size_t> nextStart(0);
    auto partitioner = [&nextStart] (IPv4Col* tablet, PartitionByDest* parts,
            uint16_t maxTargets, uint8_t numWorkers, uint8_t workerId) {
        timeTrace("worker %u spawned on core %d", workerId, Arachne::core.id);
        const size_t BATCH_SIZE = tablet->size() / (numWorkers * 20);

        size_t numRecords = 0;
        while (true) {
            size_t start = nextStart.fetch_add(BATCH_SIZE);
            size_t end = std::min(start + BATCH_SIZE, tablet->size());
            if (start >= tablet->size()) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                size_t target = (*tablet)[i] % maxTargets;
                (*parts)[target].appendCopy(&(*tablet)[i]);
            }
            numRecords += (end - start);
        }
        timeTrace("worker done, processed %u records", numRecords);
    };

    ShuffleOp* shuffleOp;
    {
        SCOPED_TIMER(bigQueryStep1ElapsedTime)
        std::vector<Arachne::ThreadId> workers;
        uint8_t id = 0;
        for (int coreId : coresAvail) {
            workers.push_back(Arachne::createThreadOnCore(coreId, partitioner,
                    &tablet, msgsPerCore[id].get(), uint16_t(world->size()),
                    coresAvail.size(), id));
            id++;
        }
        for (auto &tid : workers) {
            Arachne::join(tid);
        }
        timeTrace("step 1: hash-partitioned ipv4 addresses");

        shuffleOp = new ShuffleOpOpt(ALLSHUFFLE_BIGQUERY_0, world.get(),
                (char*) incomingKeys, ~0lu);
        for (size_t target = 0; target < world->size(); target++) {
            for (size_t c = 0; c < coresAvail.size(); c++) {
                shuffleOp->outgoingMessages[target].appendExternal(
                        &msgsPerCore[c]->at(target));
            }
//            timeTrace("step 1: sending %u bytes to node %u",
//                    shuffleOp->outgoingMessages[target].size(), target);
        }
    }

    // Step 2: shuffle the records s.t. all records of the same IPv4 address
    // end up on the same node.
    {
        SCOPED_TIMER(bigQueryStep2ElapsedTime)
        globalShuffleOpTable[shuffleOp->opId] = shuffleOp;
        invokeShufflePush(shuffleOp->group, shuffleOp->opId,
                &shuffleOp->outgoingMessages, shuffleOp);
        // Note: we can't delete this shuffle op yet; suppose some other node X
        // issues a ShufflePushRpc to us and we piggyback some data for X in the
        // reply but the reply gets lost: from our perspective, this shuffle op
        // is done (i.e., we received and sent all data) but when X retries the
        // RPC we will have no record for it...
//        globalShuffleOpTable[shuffleOp->opId] = NULL;
        timeTrace("step 2: finished shuffling ipv4 addresses");
    }

    // Step 3: sort all incoming shuffled records so that we can count the
    // ocurrences of each IP address by scanning the records sequentially;
    // without sorting, we would have to use a huge hash table instead, which
    // is likely going to be much slower due to cache misses.

    // List of (count, IPv4)-pairs.
    using CountIpList = std::vector<std::pair<uint64_t, Ipv4Address>>;

    auto scanner = [] (Ipv4Address* ipAddrs, size_t cnt, CountIpList* topIPs) {
        timeTrace("scanner spawned on core %d", Arachne::core.id);
        assert(cnt > 0);
        CountIpList ipCnt;
        ipCnt.reserve(cnt);
        Ipv4Address current = *ipAddrs;
        ipCnt.emplace_back(1, current);
        for (size_t i = 1; i < cnt; i++) {
            if (current == ipAddrs[i]) {
                ipCnt.back().first++;
            } else {
                ipCnt.emplace_back(1, ipAddrs[i]);
                current = ipAddrs[i];
            }
        }
        timeTrace("scanned %u IP's, found %u unique", cnt, ipCnt.size());

        // Always keep the first and last IP's because # edits contributed by
        // these two IP's could be incomplete yet.
        topIPs->clear();
        CountIpList tmp;
        if (ipCnt.size() > 1) {
            tmp.push_back(ipCnt[0]);
            ipCnt[0].first = 0;
            tmp.push_back(ipCnt.back());
            ipCnt.pop_back();
        }

        // Perform a partial-sort to get the top-K contributor IP addresses
        size_t n = std::min(ipCnt.size(), LIMIT_K);
        topIPs->resize(n);
        std::partial_sort_copy(ipCnt.begin(), ipCnt.end(),
                topIPs->begin(), topIPs->end(),
                std::greater<CountIpList::value_type>());
        topIPs->insert(topIPs->end(), tmp.begin(), tmp.end());
        timeTrace("scanner done, retained %u ipv4 addresses", topIPs->size());
    };

    size_t numIPs = shuffleOp->totalRxBytes.load() / sizeof(Ipv4Address);
    CountIpList topIPs;
    if (numIPs > 0) {
        SCOPED_TIMER(bigQueryStep3ElapsedTime);
        Ipv4Address* incomingIPs = (Ipv4Address*) incomingKeys;
        ips4o::parallel::sort(incomingIPs, incomingIPs + numIPs,
                std::less<Ipv4Address>(), coresAvail.size());
        timeTrace("step 3: sorted %u ipv4 addresses w/ %u cores", numIPs,
                coresAvail.size());

        // Scan all IP's in parallel to determine unique IP's.
        std::vector<CountIpList> topIPsByCore(coresAvail.size());
        std::vector<Arachne::ThreadId> workers;
        Ipv4Address* rangeStart = incomingIPs;
        size_t addrsPerCore =
                (numIPs + coresAvail.size() - 1) / coresAvail.size();
        size_t addrsLeft = numIPs;
        int id = 0;
        for (int coreId : coresAvail) {
            size_t n = std::min(addrsPerCore, addrsLeft);
            workers.push_back(Arachne::createThreadOnCore(coreId, scanner,
                    rangeStart, n, &topIPsByCore[id]));
            rangeStart += n;
            addrsLeft -= n;
            id++;
        }
        for (auto &tid : workers) {
            Arachne::join(tid);
        }
        timeTrace("step 3: each core scanned a portion of ipv4 addresses");

        // Combine top K contributing IP addresses found by each core to compute
        // the overall top K IP addresses.
        robin_hood::unordered_map<Ipv4Address, uint64_t> ipCounter;
        for (CountIpList& list : topIPsByCore) {
            for (auto& kv : list) {
                ipCounter[kv.second] += kv.first;
            }
        }
        for (auto& kv : ipCounter) {
            topIPs.emplace_back(kv.second, kv.first);
        }
        std::sort(topIPs.begin(), topIPs.end(),
                std::greater<CountIpList::value_type>());
        topIPs.resize(std::min(topIPs.size(), LIMIT_K));
    }
    timeTrace("step 3: retained %u ipv4 addresses", topIPs.size());

    // Step 5: reduce/aggregate partial results on each node to compute the
    // global top-K IP addresses.

    /// Compute the top-K IPv4 addresses that contribute the most edits.
    struct TopContributors : public TreeGather::Merger {
        /// Used to hold the top-K IPv4 addresses that are aggregated over
        /// partial results from children nodes.
        CountIpList* topIPs;

        /// Serializes #topAddrs in wire format so it can be sent over the
        /// network.
        Buffer result;

        /// Serializes the calls to #add.
        Arachne::SpinLock mutex;

        explicit TopContributors(CountIpList* topIPs)
                : topIPs(topIPs), result(), mutex() {}

        void add(Buffer* partialResult) {
            timeTrace("TopContributor::add invoked, buffer size %u",
                    partialResult->size());
            std::lock_guard<Arachne::SpinLock> lock(mutex);
            uint32_t offset = 0;
            while (offset < partialResult->size()) {
                uint64_t cnt = *partialResult->read<uint64_t>(&offset);
                Ipv4Address ip = *partialResult->read<Ipv4Address>(&offset);
                topIPs->emplace_back(cnt, ip);
            }

            size_t n = std::min(topIPs->size(), LIMIT_K);
            std::partial_sort(topIPs->begin(), topIPs->begin() + n,
                    topIPs->end(), std::greater<CountIpList::value_type>());
            topIPs->resize(n);
            timeTrace("TopContributor::add finished");
        }

        Buffer* getResult() {
            if (result.size() == 0) {
                for (auto& kv : *topIPs) {
                    result.appendCopy(&kv.first);
                    result.appendCopy(&kv.second);
                }
            }
            return &result;
        }
    };

    const int DUMMY_OP_ID = 999;
    TopContributors topContributors(&topIPs);
    {
        SCOPED_TIMER(bigQueryStep4ElapsedTime)
        Tub<TreeGather> gatherOp;
        invokeCollectiveOp<TreeGather>(gatherOp, DUMMY_OP_ID, context,
                world.get(), 0, &topContributors);
        gatherOp->wait();
        removeCollectiveOp(DUMMY_OP_ID);
        timeTrace("step 5: gather op completed");
    }
    uint64_t elapsed = Cycles::rdtsc() - startTime - initTime;
    ADD_COUNTER(bigQueryTime, elapsed);

    // Pretty-print the query result that will be returned to the BQ client
    if (world->rank == 0) {
        std::string prettyPrint;
        for (auto& kv : topIPs) {
            Ipv4Address ip = kv.second;
            std::string ipStr = format("%u.%u.%u.%u", (ip>>24)&0xff,
                    (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff);
            prettyPrint += format("%s, %lu | ", ipStr.c_str(), kv.first);
        }
        LOG(NOTICE, "BigQuery Q2 result (scan %d records in %.3f ms):\n%s",
                numDataTuples * world->size(), Cycles::toSeconds(elapsed)*1e3,
                prettyPrint.c_str());
    } else {
        LOG(NOTICE, "Finished processing BigQuery Q2");
    }
}

void
MilliSortService::bigQueryQ3()
{
    static const int LIMIT_K = 10;

    /// Customized std::string_view with our own comparison functions (we don't
    /// really need lexicographical order). We used StringView instead of string
    /// in many places in order to avoid string construction cost (e.g., during
    /// shuffle message deserialization, sorting, etc.)
    struct StringView {
        char* s;
        uint32_t len;

        StringView()
            : s(), len() {}

        StringView(char* s, uint32_t len)
            : s(s), len(len) {}

        uint64_t hash()
        {
            std::string_view sv(s, len);
            return std::hash<std::string_view>()(sv);
        }

        int compare(const StringView& other) const
        {
            if (len == other.len) {
                return strncmp(s, other.s, len);
            } else {
                return int(len) - int(other.len);
            }
        }

        bool operator<(const StringView& other) const
        {
            if (len == other.len) {
                return strncmp(s, other.s, len) < 0;
            } else {
                return len < other.len;
            }
        }

        bool operator==(const StringView& other) const
        {
            return compare(other) == 0;
        }

        bool operator!=(const StringView& other) const
        {
            return compare(other) != 0;
        }
    };

    using StringCol = std::vector<std::string>;
    using IntCol = std::vector<uint64_t>;

    /// Columnar table holding (repo, author) records.
    using CommitTable = std::pair<StringCol, StringCol>;
    /// Columnar table holding (repo, lang, bytes) records.
    using LanguageTable = std::tuple<StringCol, StringCol, IntCol>;

    /// A record view in #CommitTable (i.e., do not own data of string type)
    using RepoAuthorRecView = std::pair<StringView, StringView>;
    /// A record view in #LanguageTable.
    using RepoLangBytesRecView = std::tuple<StringView, StringView, uint64_t>;
    /// A record view in the result table resulted from joining #CommitTable
    /// and #LanguageTable on repo.
    using LangAuthorBytesRecView = std::tuple<StringView, StringView, uint64_t>;

    // Step 0: initialize input data
    static CommitTable rawCommits;
    static LanguageTable repoLangs;
    static const int recordsPerTablet = 10*1000000;
    uint64_t initTime = 0;
    if (rawCommits.first.size() != numDataTuples) {
        CycleCounter<> _(&initTime);
        // Initialize the commit tablet
        StringCol& repoCol1 = rawCommits.first;
        StringCol& authorCol = rawCommits.second;
        repoCol1.clear();
        authorCol.clear();
        repoCol1.reserve(numDataTuples);
        authorCol.reserve(numDataTuples);
        int sid = serverId.indexNumber() - 1;
        int startRecId = sid * numDataTuples;
        int stopRecId = (sid + 1) * numDataTuples - 1;
        size_t totalRecords = 0;
        for (int tabletId = startRecId / recordsPerTablet;
                tabletId <= stopRecId / recordsPerTablet; tabletId++) {
            int curRecId = tabletId * recordsPerTablet - 1;
            string filename = format(
                    "/home/yilongl/work/github/commit-tablet-%03d.csv", tabletId);
            std::ifstream file(filename.c_str());
            if (file.is_open()) {
                std::string line;
                std::string token;
                while (std::getline(file, line)) {
                    curRecId++;
                    if (curRecId < startRecId) continue;
                    if (curRecId > stopRecId) break;

                    size_t comma = line.find(',');
                    if (comma == string::npos) continue;
                    std::string repo = line.substr(0, comma);
                    std::string author = line.substr(comma + 1);
                    repoCol1.emplace_back(repo);
                    authorCol.emplace_back(author);
                }
            } else {
                LOG(ERROR, "File not found: %s", filename.c_str());
            }
            LOG(NOTICE, "Initialized commit-tablet with %lu records from %s",
                    repoCol1.size() - totalRecords, filename.c_str());
            totalRecords = repoCol1.size();
        }

        // Initialize the repo-lang tablet
        StringCol& repoCol2 = std::get<0>(repoLangs);
        StringCol& langCol = std::get<1>(repoLangs);
        IntCol& bytesCol = std::get<2>(repoLangs);
        repoCol2.clear();
        langCol.clear();
        bytesCol.clear();
        std::string repoLangFile("/home/yilongl/work/github/repo-lang.csv");
        Tub<std::ifstream> file;
        file.construct(repoLangFile.c_str());
        if (file->is_open()) {
            // Count # records in the repo-languages table
            size_t numRows = 0;
            std::string line;
            while (std::getline(*file.get(), line)) ++numRows;

            // Read the portion of the records assigned to us.
            file.construct(repoLangFile.c_str());
            int ntuples = (numRows + world->size() - 1) / world->size();
            startRecId = sid * ntuples;
            stopRecId = (sid + 1) * ntuples - 1;
            int curRecId = -1;
            while (std::getline(*file.get(), line)) {
                curRecId++;
                if (curRecId < startRecId) continue;
                if (curRecId > stopRecId) break;

                try {
                    size_t comma1 = line.find_first_of(',');
                    size_t comma2 = line.find_last_of(',');
                    if ((comma1 == string::npos) || (comma2 == string::npos))
                        continue;
                    std::string lang = line.substr(0, comma1);
                    size_t bytes = std::stoll(line.substr(comma1 + 1, comma2));
                    std::string repo = line.substr(comma2 + 1);
                    repoCol2.emplace_back(repo);
                    langCol.emplace_back(lang);
                    bytesCol.emplace_back(bytes);
                } catch (...) {
                    LOG(WARNING, "Couldn't parse record %s", line.c_str());
                }
            }
        } else {
            LOG(ERROR, "File not found: %s", repoLangFile.c_str());
        }
        LOG(NOTICE, "Initialized repo-lang tablet with %lu records from %s",
                langCol.size(), repoLangFile.c_str());
    }

    // Step 1: scan the local tablet to build up the shuffle messages; use all
    // worker cores to parallelize the work.
    struct CommitRecordPartitioner : public HashPartitioner {
        StringCol& repoCol;
        StringCol& authorCol;

        CommitRecordPartitioner(size_t nodes, std::vector<int>* coresAvail,
                CommitTable* commitTable)
            : HashPartitioner(commitTable->first.size(), nodes, coresAvail)
            , repoCol(commitTable->first)
            , authorCol(commitTable->second)
        {}

        virtual void
        doWork(std::vector<Buffer>* outMsgs, size_t recordId) override
        {
            std::string& repo = repoCol[recordId];
            std::string& author = authorCol[recordId];
            // TODO: use lightweight string hash function?
            size_t h = std::hash<std::string>()(repo);
            Buffer* target = &outMsgs->at(h % outMsgs->size());
            target->appendCopy(repo.c_str(), repo.size() + 1);
            target->appendCopy(author.c_str(), author.size() + 1);
        }
    };

    struct CommitRecordDeserializer : public Deserializer<RepoAuthorRecView> {

        CommitRecordDeserializer(std::vector<int>* coresAvail,
                std::vector<std::vector<char>>* incomingMessages)
            : Deserializer<RepoAuthorRecView>(coresAvail, incomingMessages)
        {}

        virtual void process(size_t msgId,
                std::vector<RepoAuthorRecView>* outRecords)
        {
            vector<char>& msg = incomingMessages->at(msgId);
            uint32_t len;
            char* old;
            char* s = msg.data();
            while (s < msg.data() + msg.size()) {
                // (repo, author)
                old = nextString(s, len);
                StringView repo(old, len);
                old = nextString(s, len);
                StringView author(old, len);
                outRecords->emplace_back(repo, author);
            }
        }
    };

    // Step 1: shuffle commit records based on their repos; that is, all records
    // with the same repo shall end up on the same node.
    ShuffleOp* shuffleOp0 = new ShuffleOp(ALLSHUFFLE_BIGQUERY_0, world.get());
    CommitRecordPartitioner serializeCommits(world->size(), &coresAvail,
            &rawCommits);
    CommitRecordDeserializer commitDeserializer(&coresAvail,
            &shuffleOp0->incomingMessages);
    {
        SCOPED_TIMER(bigQueryStep1ElapsedTime)
        serializeCommits.start(&shuffleOp0->outgoingMessages);
        timeTrace("step 1: hash-partitioned the commit records based on repo");

        globalShuffleOpTable[shuffleOp0->opId] = shuffleOp0;
        invokeShufflePush(shuffleOp0->group, shuffleOp0->opId,
                &shuffleOp0->outgoingMessages, shuffleOp0);
        timeTrace("step 1: shuffled commit records, outgoing %u",
                rawCommits.first.size());

        // Deserialize commit records from incoming shuffle messages; use all
        // cores in parallel.
        commitDeserializer.start();
        timeTrace("step 1: deserialized %u incoming commit records",
                commitDeserializer.numRecords);
    }

    // Step 2: remove duplicate incoming commit records; this is implemented by
    // sorting and then scanning the commit records (in parallel). Note: another
    // solution is to use a huge hash table, but this is likely going to be much
    // slower due to cache misses.
    struct RepoAuthorCombiner : public Combiner {

        std::vector<RepoAuthorRecView*>* commitPtrs;

        std::unique_ptr<RepoAuthorRecView*[]> leftJoinTable;

        explicit RepoAuthorCombiner(std::vector<RepoAuthorRecView*>* commitPtrs,
                std::vector<int>* coresAvail)
            : Combiner(commitPtrs->size(), coresAvail)
            , commitPtrs(commitPtrs)
            , leftJoinTable(new RepoAuthorRecView*[commitPtrs->size()])
        {}

        virtual bool equal(size_t idx0, size_t idx1)
        {
            return *(*commitPtrs)[idx0] == *(*commitPtrs)[idx1];
        }

        virtual void doWork(size_t recordId, size_t& lastUniq) override
        {
            if (!equal(recordId, lastUniq)) {
                (*commitPtrs)[++lastUniq] = (*commitPtrs)[recordId];
            }
        }

        virtual void copyRecord(size_t fromIdx, size_t toIdx) override
        {
            leftJoinTable[toIdx] = (*commitPtrs)[fromIdx];
        }
    };

    Tub<RepoAuthorCombiner> repoAuthorCombiner;
    {
        SCOPED_TIMER(bigQueryStep2ElapsedTime)
        std::vector<RepoAuthorRecView*> commitPtrs;
        commitPtrs.reserve(commitDeserializer.numRecords);
        for (auto& vec : commitDeserializer.perCoreRecords) {
            for (auto& rec : vec) {
                commitPtrs.push_back(&rec);
            }
        }
        timeTrace("step 2: initialized ptr array for indirect sort");
        ips4o::parallel::sort(commitPtrs.begin(), commitPtrs.end(),
                [] (auto x, auto y) {
                    int r = x->first.compare(y->first);
                    return (r != 0) ? (r < 0) : (x->second.compare(y->second) < 0);
                },
                coresAvail.size());

        timeTrace("step 2: sorted %u commit records", commitPtrs.size());

        repoAuthorCombiner.construct(&commitPtrs, &coresAvail);
        repoAuthorCombiner->run();
        timeTrace("step 2: found %u unique (repo, author) records",
                repoAuthorCombiner->numFinalRecords);
    }

    // Step 3: shuffle the language table based on repo as well (so each node
    // can later perform the join operation independently).
    struct LangRecordPartitioner : public HashPartitioner {
        StringCol& repoCol = std::get<0>(repoLangs);
        StringCol& langCol = std::get<1>(repoLangs);
        IntCol& bytesCol = std::get<2>(repoLangs);

        LangRecordPartitioner(size_t nodes, std::vector<int>* coresAvail,
                LanguageTable* langTable)
            : HashPartitioner(std::get<0>(*langTable).size(), nodes, coresAvail)
            , repoCol(std::get<0>(*langTable))
            , langCol(std::get<1>(*langTable))
            , bytesCol(std::get<2>(*langTable))
        {}

        virtual void
        doWork(std::vector<Buffer>* outMsgs, size_t i) override
        {
            size_t h = std::hash<std::string>()(repoCol[i]) % outMsgs->size();
            Buffer* target = &outMsgs->at(h);
            target->appendCopy(repoCol[i].c_str(), repoCol[i].size() + 1);
            target->appendCopy(langCol[i].c_str(), langCol[i].size() + 1);
            target->appendCopy(&bytesCol[i]);
        }
    };

    struct LangRecordDeserializer : public Deserializer<RepoLangBytesRecView> {

        LangRecordDeserializer(std::vector<int>* coresAvail,
                std::vector<std::vector<char>>* incomingMessages)
            : Deserializer<RepoLangBytesRecView>(coresAvail, incomingMessages)
        {}

        virtual void process(size_t msgId,
                std::vector<RepoLangBytesRecView>* outRecords)
        {
            vector<char>& msg = incomingMessages->at(msgId);
            uint32_t len;
            char* old;
            char* s = msg.data();
            while (s < msg.data() + msg.size()) {
                // (repo, lang, bytes)
                old = nextString(s, len);
                StringView repo(old, len);
                old = nextString(s, len);
                StringView lang(old, len);
                outRecords->emplace_back(repo, lang, next8BInt(s));
            }
        }
    };

    LangRecordPartitioner serializeLangs(world->size(), &coresAvail,
            &repoLangs);
    ShuffleOp* shuffleOp1 = new ShuffleOp(ALLSHUFFLE_BIGQUERY_2, world.get());
    LangRecordDeserializer langDeserializer(&coresAvail,
            &shuffleOp1->incomingMessages);
    {
        SCOPED_TIMER(bigQueryStep3ElapsedTime)
        serializeLangs.start(&shuffleOp1->outgoingMessages);
        timeTrace("step 3: hash-partitioned the language records");

        globalShuffleOpTable[shuffleOp1->opId] = shuffleOp1;
        invokeShufflePush(shuffleOp1->group, shuffleOp1->opId,
                &shuffleOp1->outgoingMessages, shuffleOp1);
        timeTrace("step 3: shuffled the language records");

        langDeserializer.start();
        timeTrace("step 3: deserialized %u language records",
                langDeserializer.numRecords);
    }
//    for (auto& tuple : incomingLangs) {
//        LOG(NOTICE, "lang (%s, %s, %lu)", std::get<0>(tuple).c_str(),
//                std::get<1>(tuple).c_str(), std::get<2>(tuple));
//    }

    // Step 4: join the commit table and the language table on repo
    std::atomic<size_t> nextStart(0);
    vector<vector<LangAuthorBytesRecView>> joinResults(coresAvail.size());
    auto joiner = [&nextStart, &repoAuthorCombiner]
            (std::vector<RepoLangBytesRecView*>* rightJoinTable,
            std::vector<LangAuthorBytesRecView>* output) {
        RepoAuthorRecView** leftJoinTable =
                repoAuthorCombiner->leftJoinTable.get();
        size_t numLeftJoinRecords = repoAuthorCombiner->numFinalRecords;

        while (true) {
            const static size_t B = 4096;
            size_t start = nextStart.fetch_add(B);
            size_t end = std::min(start + B, numLeftJoinRecords);
            if (start >= numLeftJoinRecords) {
                break;
            }

            auto leftIt = &leftJoinTable[start];
            auto rightIt = std::lower_bound(rightJoinTable->begin(),
                    rightJoinTable->end(), std::get<0>(**leftIt),
                    [] (const RepoLangBytesRecView* v, const StringView& repo)
                    { return std::get<0>(*v).compare(repo) < 0; }
            );
            auto leftItEnd = &leftJoinTable[end];
            auto rightItEnd = rightJoinTable->end();
            while ((leftIt != leftItEnd) && (rightIt != rightItEnd)) {
                StringView& repo = std::get<0>(**leftIt);
                int cmp = repo.compare(std::get<0>(**rightIt));
                if (cmp < 0) {
                    leftIt++;
                    continue;
                } else if (cmp > 0) {
                    rightIt++;
                    continue;
                } else {
                    auto leftIt0 = leftIt + 1;
                    auto rightIt0 = rightIt + 1;
                    while ((leftIt0 != leftItEnd) &&
                           (repo == std::get<0>(**leftIt0))) {
                        leftIt0++;
                    }
                    while ((rightIt0 != rightItEnd) &&
                           (repo == std::get<0>(**rightIt0))) {
                        rightIt0++;
                    }
                    for (auto l = leftIt; l != leftIt0; l++) {
                        for (auto r = rightIt; r != rightIt0; r++) {
                            output->emplace_back(std::get<1>(**r),
                                    std::get<1>(**l), std::get<2>(**r));
                        }
                    }
                    leftIt = leftIt0;
                    rightIt = rightIt0;
                }
            }
        }
    };

    struct HashSumBytes : public HashCombiner {

        struct KeyHash {
            size_t operator()(LangAuthorBytesRecView* view) const {
                return std::get<0>(*view).hash() ^ std::get<1>(*view).hash();
            }
        };

        struct KeyEqual {
            bool operator()(LangAuthorBytesRecView* x,
                    LangAuthorBytesRecView* y) const {
                return (std::get<0>(*x) == std::get<0>(*y)) &&
                       (std::get<1>(*x) == std::get<1>(*y));
            }
        };

        struct DummyAllocator {

            HashSumBytes* owner;

            void* allocate(size_t size) {
                char* result = static_cast<char*>(owner->memOffset);
                owner->memOffset = result + size;
                return result;
            }

            void deallocate(void* p, size_t size) { }
        };

        DummyAllocator dummyAlloc;

        /// Memory backing #finalRecords and #map.
        void* backMemory;

        /// First byte available in #backMemory.
        void* memOffset;

        /// # bytes in #backMemory.
        size_t memBytes;

        /// Array of pointers to input records.
        LangAuthorBytesRecView** recordPtrs;

        /// Array of final records located inside #backingMemory.
        LangAuthorBytesRecView* finalRecords;

        folly::AtomicUnorderedInsertMap<LangAuthorBytesRecView*,
                folly::MutableAtom<size_t>, KeyHash, KeyEqual, true,
                std::atomic, uint32_t, DummyAllocator> map;

        std::atomic<size_t> numCopied;

        using MapIter = decltype(map)::const_iterator;

        HashSumBytes(LangAuthorBytesRecView** recordPtrs, size_t nrecords,
                std::vector<int>* coresAvail, void* backMemory, size_t memBytes)
            : HashCombiner(nrecords, coresAvail)
            , dummyAlloc{this}
            , backMemory(backMemory)
            , memOffset(backMemory)
            , memBytes(memBytes)
            , recordPtrs(recordPtrs)
            , finalRecords(NULL)
            , map(nrecords, 0.8f, dummyAlloc)
            , numCopied(0)
        {
            finalRecords = static_cast<LangAuthorBytesRecView*>(memOffset);
            char* endOffset = (char*)(finalRecords + nrecords);
            if (endOffset > (char*) backMemory + memBytes) {
                DIE("Not enough memory for finalRecords, backMemory %p, "
                    "memBytes %lu, endOffset %p", backMemory, memBytes,
                    endOffset);
            }
        }

        virtual bool insert(size_t recordId) override
        {
            LangAuthorBytesRecView* rec = recordPtrs[recordId];
            auto result = map.findOrConstruct(rec, [] (void* raw) {
                new (raw) folly::MutableAtom<size_t>(0);
            });
            result.first->second.data.fetch_add(std::get<2>(*rec),
                    std::memory_order_relaxed);
            return result.second;
        }

        virtual void copyRecords(size_t shardId) override
        {
            auto range = map.shardRange(numMapShards, shardId);
            size_t cnt = 0;
            for (MapIter it(range.first); it != range.second; ++it) {
                cnt++;
            }

            size_t copyDst = numCopied.fetch_add(cnt);
            for (MapIter it(range.first); it != range.second; ++it) {
                finalRecords[copyDst] = *it->first;
                std::get<2>(finalRecords[copyDst]) =
                        it->second.data.load(std::memory_order_relaxed);
                copyDst++;
            }
        }
    };

    std::vector<RepoLangBytesRecView*> rightJoinTable;
    rightJoinTable.reserve(langDeserializer.numRecords);
    static const size_t BACK_MEMORY_BYTES = 100 * 1000 * 1000;
    static void* sumBytesBackMemory = new char[BACK_MEMORY_BYTES];
    Tub<HashSumBytes> sumBytes;
    {
        SCOPED_TIMER(bigQueryStep4ElapsedTime)

        // By now, the leftJoinTable should've been sorted according to repo;
        // we just need to sort the rightJoinTable too.
        // TODO: parallelize?
        for (auto& vec : langDeserializer.perCoreRecords) {
            for (auto& rec : vec) {
                rightJoinTable.push_back(&rec);
            }
        }
        timeTrace("step 4: prepared ptr array for indirect sort");
        ips4o::parallel::sort(rightJoinTable.begin(), rightJoinTable.end(),
                [] (const auto* x, const auto* y) {
                    return std::get<0>(*x).compare(std::get<0>(*y)) < 0;
                },
                coresAvail.size());
        timeTrace("step 4: sorted %u records in rightJoinTable based on repo",
                rightJoinTable.size());

        // The following implements the mergesort-join algorithm.
        std::vector<Arachne::ThreadId> workers;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    joiner, &rightJoinTable, &joinResults[id]));
        }
        for (auto& tid : workers) Arachne::join(tid);

        size_t numJoinedRecords = 0;
        for (auto& vec : joinResults) {
            numJoinedRecords += vec.size();
        }
        timeTrace("step 4: joined the commit and language tables on repo");

        // Indirect-sort based on (lang, author)
        workers.clear();
        std::unique_ptr<LangAuthorBytesRecView*[]> joinResultPtrs(
                new LangAuthorBytesRecView*[numJoinedRecords]);
        size_t startIdx = 0;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    [&joinResultPtrs] (std::vector<LangAuthorBytesRecView>* in,
                            size_t idx)
                    { for (auto& r : *in) joinResultPtrs[idx++] = &r; },
                    &joinResults[id], startIdx));
            startIdx += joinResults[id].size();
        }
        for (auto& tid : workers) Arachne::join(tid);
        timeTrace("step 4: prepared ptr array for indirect sort");

        sumBytes.construct(joinResultPtrs.get(), numJoinedRecords,
                &coresAvail, sumBytesBackMemory, BACK_MEMORY_BYTES);
        timeTrace("step 4: constructed sumBytes (folly::AtomicUnorderedMap)");
        sumBytes->run();
        timeTrace("step 4: summed up bytes for %u (lang, author) combinations",
                sumBytes->numFinalRecords);
    }
//    for (auto& tuple : localGroupBy) {
//        LOG(NOTICE, "result (%s, %s, %lu)", std::get<0>(tuple).c_str(),
//                std::get<1>(tuple).c_str(), std::get<2>(tuple));
//    }

    // Step 5: shuffle the output table produced by join based on (lang, author)
    // in order to sum up the bytes.
    struct JoinResultPartitioner : public HashPartitioner {
        LangAuthorBytesRecView* localGroupBy;

        JoinResultPartitioner(size_t nrecords, size_t nodes,
                std::vector<int>* coresAvail,
                LangAuthorBytesRecView* localGroupBy)
            : HashPartitioner(nrecords, nodes, coresAvail)
            , localGroupBy(localGroupBy)
        {}

        virtual void
        doWork(std::vector<Buffer>* outMsgs, size_t i) override
        {
            StringView lang, author;
            uint64_t bytes;
            std::tie(lang, author, bytes) = localGroupBy[i];
            size_t h = (lang.hash() ^ author.hash()) % outMsgs->size();
            Buffer* buffer = &outMsgs->at(h);
            char nullChar = 0;
            buffer->appendCopy(lang.s, lang.len);
            buffer->appendCopy(&nullChar);
            buffer->appendCopy(author.s, author.len);
            buffer->appendCopy(&nullChar);
            buffer->appendCopy(&bytes);

        }
    };

    struct JoinResultDeserializer : public Deserializer<LangAuthorBytesRecView>
    {
        JoinResultDeserializer(std::vector<int>* coresAvail,
                std::vector<std::vector<char>>* incomingMessages)
            : Deserializer<LangAuthorBytesRecView>(coresAvail, incomingMessages)
        {}

        virtual void process(size_t msgId,
                std::vector<RepoLangBytesRecView>* outRecords)
        {
            vector<char>& msg = incomingMessages->at(msgId);
            uint32_t len;
            char* old;
            char* s = msg.data();
            while (s < msg.data() + msg.size()) {
                // (lang, author, bytes)
                old = nextString(s, len);
                StringView lang(old, len);
                old = nextString(s, len);
                StringView author(old, len);
                outRecords->emplace_back(lang, author, next8BInt(s));
            }
        }
    };

    JoinResultPartitioner joinResultPartitioner(sumBytes->numFinalRecords,
            world->size(), &coresAvail, sumBytes->finalRecords);
    ShuffleOp* shuffleOp2 = new ShuffleOp(ALLSHUFFLE_BIGQUERY_3, world.get());
    JoinResultDeserializer joinResultDeserializer(&coresAvail,
            &shuffleOp2->incomingMessages);
    std::vector<LangAuthorBytesRecView> topBytes(LIMIT_K);
    static void* sumBytes2BackMemory = new char[BACK_MEMORY_BYTES];
    Tub<HashSumBytes> sumBytes2;
    {
        SCOPED_TIMER(bigQueryStep5ElapsedTime)
        joinResultPartitioner.start(&shuffleOp2->outgoingMessages);
        timeTrace("step 5: hash-partitioned join result based on (lang, author)");

        globalShuffleOpTable[shuffleOp2->opId] = shuffleOp2;
        invokeShufflePush(shuffleOp2->group, shuffleOp2->opId,
                &shuffleOp2->outgoingMessages, shuffleOp2);
        timeTrace("step 5: shuffled the join result by (lang, author)");

        joinResultDeserializer.start();
        timeTrace("step 5: deserialized %u (lang, author, bytes) records",
                joinResultDeserializer.numRecords);

        // Indirect-sort based on (lang, author)
        std::vector<LangAuthorBytesRecView*> groupBySumPtrs;
        groupBySumPtrs.reserve(joinResultDeserializer.numRecords);
        // TODO: parallelize?
        for (auto& vec : joinResultDeserializer.perCoreRecords) {
            for (auto& rec : vec) {
                groupBySumPtrs.push_back(&rec);
            }
        }
        timeTrace("step 5: prepared groupBySumPtrs");

        // Summed up bytes for each (lang, author); use all cores in parallel
        sumBytes2.construct(groupBySumPtrs.data(), groupBySumPtrs.size(),
                &coresAvail, sumBytes2BackMemory, BACK_MEMORY_BYTES);
        timeTrace("step 5: constructed HashSumBytes");
        if (!groupBySumPtrs.empty()) {
            sumBytes2->run();
        }
        size_t recordsLeft = sumBytes2->numFinalRecords;
        timeTrace("step 5: merge duplicate records, before %u, after %u",
                sumBytes2->numRecords, recordsLeft);

        // Sort by bytes in descending order
        size_t n = std::min(recordsLeft, size_t(LIMIT_K));
        std::partial_sort_copy(sumBytes2->finalRecords,
                sumBytes2->finalRecords + recordsLeft,
                topBytes.begin(), topBytes.end(),
                [] (const auto& lhs, const auto& rhs)
                { return std::get<2>(lhs) > std::get<2>(rhs); }
        );
        topBytes.resize(n);
        timeTrace("step 5: partial-sorted top %u records", n);
    }

    // Step 6: reduce to compute the top-K (lang, author, bytes)-tuples
    /// Compute the top-K IPv4 addresses that contribute the most edits.
    using LangAuthorBytes = std::tuple<std::string, std::string, uint64_t>;
    struct TopBytes : public TreeGather::Merger {
        /// Used to hold the top-K (lang, author, bytes) tuples aggregated over
        /// partial results from children nodes.
        std::vector<LangAuthorBytes> topBytes;

        /// Serializes #topBytes in wire format so it can be sent over the
        /// network.
        Buffer result;

        /// Serializes the calls to #add.
        Arachne::SpinLock mutex;

        explicit TopBytes(std::vector<LangAuthorBytesRecView>* topBytesView)
                : topBytes(), result(), mutex()
        {
            for (auto& tuple : *topBytesView) {
                topBytes.emplace_back(std::string(std::get<0>(tuple).s),
                        std::string(std::get<1>(tuple).s), std::get<2>(tuple));
            }
        }

        void add(Buffer* partialResult) {
            std::lock_guard<Arachne::SpinLock> lock(mutex);
            uint32_t offset = 0;
            while (offset < partialResult->size()) {
                topBytes.emplace_back("", "", 0);
                auto& entry = topBytes.back();
                while (char ch = *partialResult->read<char>(&offset))
                    std::get<0>(entry).append(1, ch);
                while (char ch = *partialResult->read<char>(&offset))
                    std::get<1>(entry).append(1, ch);
                std::get<2>(entry) = *partialResult->read<uint64_t>(&offset);
            }

            if (topBytes.size() <= LIMIT_K) return;
            std::partial_sort(topBytes.begin(), topBytes.begin() + LIMIT_K,
                    topBytes.end(), [] (const auto& lhs, const auto& rhs)
                    { return std::get<2>(lhs) > std::get<2>(rhs); }
            );
            topBytes.resize(LIMIT_K);
        }

        Buffer* getResult() {
            if (result.size() == 0) {
                for (auto& tuple : topBytes) {
                    auto& lang = std::get<0>(tuple);
                    auto& author = std::get<1>(tuple);
                    result.appendCopy(lang.c_str(), lang.size() + 1);
                    result.appendCopy(author.c_str(), author.size() + 1);
                    result.appendCopy(&std::get<2>(tuple));
                }
            }
            return &result;
        }
    };

    const int DUMMY_OP_ID = 999;
    TopBytes aggTopBytes(&topBytes);
    {
        SCOPED_TIMER(bigQueryStep6ElapsedTime)
        Tub<TreeGather> gatherOp;
        invokeCollectiveOp<TreeGather>(gatherOp, DUMMY_OP_ID, context,
                world.get(), 0, &aggTopBytes);
        gatherOp->wait();
        removeCollectiveOp(DUMMY_OP_ID);
        timeTrace("step 6: performed gather to produce the final result");
    }
    uint64_t elapsed = Cycles::rdtsc() - startTime - initTime;
    ADD_COUNTER(bigQueryTime, elapsed);

    if (world->rank == 0) {
        LOG(NOTICE, "BigQuery Q3 result (scan %d records in %.3f ms):",
                numDataTuples * world->size(), Cycles::toSeconds(elapsed)*1e3);
        for (auto& tuple : aggTopBytes.topBytes) {
            LOG(NOTICE, "(%s, %s, %lu)", std::get<0>(tuple).c_str(),
                    std::get<1>(tuple).c_str(), std::get<2>(tuple));
        }
    }
}

void
MilliSortService::bigQueryQ4()
{
    // Step 0: initialize input data
    static std::vector<std::string> titles;
    static std::vector<std::string> words;
    static const int recordsPerTablet = 10*1000000;
    uint64_t initTime = 0;
    if (titles.size() != numDataTuples) {
        CycleCounter<> _(&initTime);
        titles.clear();
        titles.reserve(numDataTuples);
        int sid = serverId.indexNumber() - 1;
        int startRecId = sid * numDataTuples;
        int stopRecId = (sid + 1) * numDataTuples - 1;
        size_t totalRecords = 0;
        for (int tabletId = startRecId / recordsPerTablet;
                tabletId <= stopRecId / recordsPerTablet; tabletId++) {
            int curRecId = tabletId * recordsPerTablet - 1;
            string filename = format(
                    "/home/yilongl/work/publicdata-samples/wiki-titles-%03d.csv", tabletId);
            std::ifstream tabletFile(filename.c_str());
            if (tabletFile.is_open()) {
                std::string line;
                while (std::getline(tabletFile, line)) {
                    curRecId++;
                    if (curRecId < startRecId) continue;
                    if (curRecId > stopRecId) break;
                    titles.emplace_back(line);
                }
            } else {
                LOG(ERROR, "File not found: %s", filename.c_str());
            }
            LOG(NOTICE, "Initialized tablet with %lu records from %s",
                    titles.size() - totalRecords, filename.c_str());
            totalRecords = titles.size();
        }
    }

    if (world->rank == 0) {
        std::string filename(
                "/home/yilongl/work/publicdata-samples/shakespeare-words.csv");
        if (words.empty()) {
            std::ifstream file(filename.c_str());
            if (file.is_open()) {
                std::string line;
                while (std::getline(file, line)) {
                    words.emplace_back(line);
                }
                LOG(NOTICE, "Initialized %lu words from %s", words.size(),
                        filename.c_str());
            } else {
                LOG(ERROR, "File not found: %s", filename.c_str());
            }
        }
    } else {
        words.clear();
    }

    // Auxilary data structure to support indirect sort on #words.
    std::vector<std::string*> wordPtrs;
    wordPtrs.reserve(words.size());
    for (std::string& word : words) {
        wordPtrs.emplace_back(&word);
    }

    // Define our customized string comparison function
    auto stringCompare = [] (const string* a, const string* b) {
        // We don't really need lexicographical order; only perform
        // the expensive char-by-char comparison when two strings
        // are equally long.
        if (a->size() == b->size()) {
            return a->compare(*b) < 0;
        } else {
            return a->size() < b->size();
        }
    };

    timeTrace("BigQuery Q4 started");

    // Step 1: compute unique words at the root; broadcast them to others
    TreeBcast bcastWordsTable(context, world.get());
    if (world->rank == 0) {
        SCOPED_TIMER(bigQueryStep1ElapsedTime)
        // Sort the words tablet to compute a small table of unique words
        // to be used in the broadcast join later
        ips4o::parallel::sort(wordPtrs.begin(), wordPtrs.end(), stringCompare,
                coresAvail.size());
        timeTrace("step 1: sorted %u words", words.size());

        // TODO: parallelize the following scan
        bigQueryQ4UniqueWords.emplace_back(*wordPtrs.front());
        for (std::string* word : wordPtrs) {
            if (bigQueryQ4UniqueWords.back().compare(*word) != 0) {
                bigQueryQ4UniqueWords.emplace_back(*word);
            }
        }
        timeTrace("step 1: got %u unique words", bigQueryQ4UniqueWords.size());

        // Broadcast the small join table of unique words
        char* buffer = (char*) localKeys;
        char* dst = buffer;
        for (std::string& word : bigQueryQ4UniqueWords) {
            memcpy(dst, word.c_str(), word.size() + 1);
            dst += word.size() + 1;
        }
        uint32_t nbytes = uint32_t(dst - buffer);
        timeTrace("step 1: about to broadcast the words table (%u bytes)",
                nbytes);
        // TODO: optimize broadcast
        bcastWordsTable.send<SendDataRpc>(BROADCAST_SHAKESPEARE_WORDS, nbytes,
                buffer);
        bcastWordsTable.wait();
    } else {
        SCOPED_TIMER(bigQueryStep1ElapsedTime)
        while (!bigQueryQ4BcastDone.load()) {
            Arachne::yield();
        }
    }
    timeTrace("step 1: received %u unique words", bigQueryQ4UniqueWords.size());

    // Step 2: join the smaller words table with the titles tablet; we use
    // a sorted list instead of a hash set to store the words table because
    // building the hash set is inherently hard to parallelize (although hash
    // set lookup is much faster than binary search).
    std::vector<std::vector<std::string>> results(coresAvail.size());
    std::atomic<size_t> nextStart(0);
    auto args = std::make_tuple(&bigQueryQ4UniqueWords, &titles, &nextStart);
    auto workerMain = [&args, &stringCompare]
            (std::vector<std::string>* result) {
        // Unpack arguments
        std::vector<std::string>* words;
        std::vector<std::string>* titles;
        std::atomic<size_t>* nextStart;
        std::tie(words, titles, nextStart) = args;

        while (true) {
            size_t start = nextStart->fetch_add(1000);
            size_t end = std::min(start + 1000, titles->size());
            if (start >= titles->size()) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                std::string& title = (*titles)[i];
                bool found = std::binary_search(words->begin(), words->end(),
                        title,
                        [&stringCompare] (const string& a, const string& b) {
                            return stringCompare(&a, &b);
                        });
                if (found) {
                    result->emplace_back(title);
                }
            }
        }
    };

    {
        SCOPED_TIMER(bigQueryStep2ElapsedTime)
        // Scan the titles tablet to filter out titles that don't exist in
        // the words table; use all cores to parallelize the work.
        int id = 0;
        std::vector<Arachne::ThreadId> workers;
        for (int coreId : coresAvail) {
            workers.push_back(Arachne::createThreadOnCore(coreId, workerMain,
                    &results[id]));
            id++;
        }
        for (auto& tid : workers) {
            Arachne::join(tid);
        }
        size_t count = 0;
        for (auto& result : results) {
            count += result.size();
        }
        timeTrace("step 2: found %u wiki titles in the words table", count);
    }
    uint64_t elapsed = Cycles::rdtsc() - startTime - initTime;
    ADD_COUNTER(bigQueryTime, elapsed);

    // The result of this query is a bit large so we are not going to aggregate
    // them back to the root (and return to the client).
    LOG(NOTICE, "Finished processing BigQuery Q4");
}

void
MilliSortService::localSortAndPickPivots()
{
    timeTrace("=== Stage 1: localSortAndPickPivots started");
    ADD_COUNTER(millisortInitItems, numDataTuples);
    ADD_COUNTER(millisortInitKeyBytes, numDataTuples * PivotKey::KEY_SIZE);
    ADD_COUNTER(millisortInitValueBytes, numDataTuples * Value::SIZE);

    auto localSort = [this] {
        ADD_COUNTER(localSortStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(localSortElapsedTime);
        int numWorkers = coresAvail.size();
        ADD_COUNTER(localSortWorkers, numWorkers);
        ips4o::parallel::sort(localKeys, localKeys + numDataTuples,
                std::less<PivotKey>(), numWorkers);
    };
    Arachne::join(Arachne::createThreadOnCore(coresAvail[0], localSort));
    timeTrace("sorted %d keys", numDataTuples);

    // Overlap rearranging initial values with subsequent stages. This only
    // needs to be done before key (actually, value) shuffle.
    rearrangeValues(rearrangeLocalVals.get(), numDataTuples, true);

#if !OVERLAP_INIT_VAL_REARRANGE
    // To avoid interfering with the partition phase, block till finish.
    uint64_t elapsedTime = rearrangeLocalVals->wait();
    timeTrace("rearranging init. values completed");
    ADD_COUNTER(rearrangeInitValuesElapsedTime, elapsedTime)
#endif

    // Pick local pivots that partitions the local keys evenly.
    partition(localKeys, numDataTuples, numPivotsPerServer,
            GRADIENT_BASED_PIVOT_SELECTION, &localPivots);
    timeTrace("picked %lu local pivots, about to gather pivots",
            localPivots.size());
    debugLogKeys("local pivots: ", &localPivots);

    // Initiate the gather op that collects pivots to the pivot server.
    // Merge pivots from slave nodes as they arrive.
    partitionStartTime = Cycles::rdtsc();
    gatheredPivots = localPivots;
    PivotMergeSorter merger(&gatheredPivots);
    {
        ADD_COUNTER(gatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherPivotsElapsedTime);
        Tub<TreeGather> gatherPivots;
        invokeCollectiveOp<TreeGather>(gatherPivots, GATHER_PIVOTS, context,
                myPivotServerGroup.get(), 0, &merger);
        gatherPivots->wait();
        // TODO: the cleanup is not very nice; how to do RAII?
        removeCollectiveOp(GATHER_PIVOTS);

        // FIXME: seems wrong division of responsibility; should I move this
        // into Gather class? Also this assumes a particular implementation of
        // Gather.
        ADD_COUNTER(gatherPivotsMergeCycles, merger.activeCycles);
        ADD_COUNTER(gatherPivotsInputBytes, merger.bytesReceived);
        ADD_COUNTER(gatherPivotsOutputBytes,
                localPivots.size() * PivotKey::SIZE);
    }

    // Pivot servers will advance to pick super pivots when the gather op
    // completes. Normal nodes will wait to receive splitters.
    if (isPivotServer) {
        timeTrace("gathered %lu pivots from %u nodes", gatheredPivots.size(),
                uint(myPivotServerGroup->size()));
        pickSuperPivots();
    } else {
        timeTrace("sent %lu local pivots", localPivots.size());
        gatheredPivots.clear();
    }
}

void
MilliSortService::pickSuperPivots()
{
    timeTrace("=== Stage 2: pickSuperPivots started");

    assert(isPivotServer);
    partition(gatheredPivots.data(), int(gatheredPivots.size()),
            allPivotServers->size(), false, &superPivots);
    timeTrace("picked %lu super-pivots, about to gather super-pivots",
            superPivots.size());
    debugLogKeys("super-pivots: ", &superPivots);

    // Initiate the gather op that collects super pivots to the root node.
    // Merge super pivots from pivot servers as they arrive.
    globalSuperPivots = superPivots;
    PivotMergeSorter merger(&globalSuperPivots);
    if (allPivotServers->size() > 1) {
        ADD_COUNTER(gatherSuperPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherSuperPivotsElapsedTime);
        Tub<TreeGather> gatherSuperPivots;
        invokeCollectiveOp<TreeGather>(gatherSuperPivots, GATHER_SUPER_PIVOTS,
                context, allPivotServers.get(), 0, &merger);
        // fixme: this is duplicated everywhere
        gatherSuperPivots->wait();
        removeCollectiveOp(GATHER_SUPER_PIVOTS);

        ADD_COUNTER(gatherSuperPivotsMergeCycles, merger.activeCycles);
        ADD_COUNTER(gatherSuperPivotsInputBytes, merger.bytesReceived);
        ADD_COUNTER(gatherSuperPivotsOutputBytes,
                superPivots.size() * PivotKey::SIZE);
    }

    // The root node will advance to pick pivot bucket boundaries when the
    // gather op completes. Other pivot servers will wait to receive pivot
    // bucket boundaries.
    if (world->rank == 0) {
        timeTrace("gathered %lu super-pivots from %u pivot servers",
                globalSuperPivots.size(), uint(allPivotServers->size()));
        pickPivotBucketBoundaries();
    } else {
        timeTrace("sent %lu super-pivots", superPivots.size());
        globalSuperPivots.clear();
    }
}

void
MilliSortService::pickPivotBucketBoundaries()
{
    timeTrace("=== Stage 3: pickPivotBucketBoundaries started");
    assert(world->rank == 0);
    partition(globalSuperPivots.data(), int(globalSuperPivots.size()),
            allPivotServers->size(), false, &pivotBucketBoundaries);
    debugLogKeys("pivot bucket boundaries: ", &pivotBucketBoundaries);

    // Broadcast computed pivot bucket boundaries to the other pivot servers
    // (excluding ourselves).
    // TODO: can we unify the API of broadcast and other collective op?
    timeTrace("picked %lu pivot bucket boundaries, about to broadcast",
            pivotBucketBoundaries.size());
    ADD_COUNTER(bcastPivotBucketBoundariesStartTime,
            Cycles::rdtsc() - startTime);
//    SCOPED_TIMER(bcastPivotBucketBoundariesElapsedTime);
    START_TIMER(bcastPivotBucketBoundariesElapsedTime);
    bcastPivotBucketBoundaries->send<SendDataRpc>(
            BROADCAST_PIVOT_BUCKET_BOUNDARIES,
            // TODO: simplify the SendDataRpc api?
            uint32_t(PivotKey::SIZE * pivotBucketBoundaries.size()),
            pivotBucketBoundaries.data());
    // Note: there is no need to wait for the broadcast to finish as we are not
    // going to modify #pivotBucketBoundaries.
//    bcastPivotBucketBoundaries->wait();
    STOP_TIMER(bcastPivotBucketBoundariesElapsedTime);

    timeTrace("broadcast to %d pivot servers", allPivotServers->size());
    pivotBucketSort();
}

void
MilliSortService::pivotBucketSort()
{
    timeTrace("=== Stage 4: pivotBucketSort started");
    assert(isPivotServer);

    // Send pivots to their designated pivot servers determined by the pivot
    // bucket boundaries.
    timeTrace("about to shuffle pivots");
    {
        ADD_COUNTER(shufflePivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(shufflePivotsElapsedTime);

        // Identify begin and end pivots in the gathered pivots.
        uint32_t reduceFactor = downCast<uint32_t>(myPivotServerGroup->size());
        std::vector<PivotTag> tags(gatheredPivots.size(), NORMAL_PIVOT);
        std::vector<int32_t> beginPivots(reduceFactor, -1);
        std::vector<int32_t> endPivots(reduceFactor, -1);
        for (size_t i = 0; i < gatheredPivots.size(); i++) {
            int rank = gatheredPivots[i].serverId - world->rank;
            if (beginPivots[rank] < 0) {
                beginPivots[rank] = i;
                tags[i] = BEGIN_PIVOT;
            }
        }
        for (size_t i = gatheredPivots.size() - 1; i > 0; i--) {
            int rank = gatheredPivots[i].serverId - world->rank;
            if (endPivots[rank] < 0) {
                endPivots[rank] = i;
                tags[i] = END_PIVOT;
            }
        }

        // Each iteration of the following loop prepares the outgoing shuffle
        // pivot message for one pivot sorter.
        uint32_t numSmallerPivots = 0;
        uint32_t numSmallerBeginPivots = 0;
        uint32_t numSmallerEndPivots = 0;
        for (int rank = 0; rank < allPivotServers->size(); rank++) {
            // Wire format of the pivot shuffle message header:
            // u32 numSmallerPivots
            //      # pivots smaller than the current bucket
            // u32 numSmallerBeginPivots
            //      # begin pivots smaller than the current bucket
            // u32 numSmallerEndPivots
            //      # end pivots smaller than the current bucket
            // u32 reduceFactor
            //      # slave servers (including itself) the pivot sorter has
            // vector<i32> beginPivots
            //      Indexes of the begin pivots in #gatheredPivots
            // vector<i32> endPivots
            //      Indexes of the end pivots in #gatheredPivots
            // These information are essential to computing the splitters on
            // pivot sorters collectively in a distributed fashion.
            Buffer* message = &pivotShuffleMessages->at(rank);
            message->appendCopy(&numSmallerPivots);
            message->appendCopy(&numSmallerBeginPivots);
            message->appendCopy(&numSmallerEndPivots);
            message->appendCopy(&reduceFactor);
            message->appendCopy(beginPivots.data(),
                    reduceFactor*sizeof(decltype(beginPivots)::value_type));
            message->appendCopy(endPivots.data(),
                    reduceFactor*sizeof(decltype(endPivots)::value_type));
            assert(message->size() == 4 * sizeof(uint32_t) +
                    2 * sizeof(uint32_t) * myPivotServerGroup->size());

            // Collect local pivots that should be sent to bucket `rank`.
            PivotKey boundary = pivotBucketBoundaries[rank];
            uint32_t k = numSmallerPivots;
            while ((k < gatheredPivots.size()) &&
                   (gatheredPivots[k] <= boundary)) {
                numSmallerBeginPivots += (tags[k] == BEGIN_PIVOT ? 1 : 0);
                numSmallerEndPivots += (tags[k] == END_PIVOT ? 1 : 0);
                k++;
            }
            message->appendCopy(&gatheredPivots[numSmallerPivots],
                    (k - numSmallerPivots) * PivotKey::SIZE);
            numSmallerPivots = k;
        }
        pivotShuffleIsReady = true;
        invokeShufflePush(allPivotServers.get(), ALLSHUFFLE_PIVOTS,
                pivotShuffleMessages.get());

        // Wait until we have received all pivots that belong to us.
        while (pivotShuffleRxCount.load() < allPivotServers->size()) {
            Arachne::yield();
        }
#if LOG_INTERMEDIATE_RESULT
        std::vector<PivotKey> tmp;
        for (auto p : sortedGatheredPivots) tmp.push_back(p.first);
        debugLogKeys("sorted pivots (local portion): ", &tmp);
#endif
    }

    // Pivot servers will advance to pick splitters when the
    // shuffle completes.
    timeTrace("finished pivot shuffle");
    pickSplitters();
}

void
MilliSortService::pickSplitters() {
    timeTrace("=== Stage 5: pickSplitters started");

    assert(isPivotServer);
#if GRADIENT_BASED_PIVOT_SELECTION
    // The following code implements the gradient-based splitter selection
    // algorithm.
    const int dataUnitsPerBucket = numPivotsPerServer * 2;
    int cmlDataUnits = numNonBeginPivotsPassed * 2 + (numActiveSources - 1);
    int nextSplitPoint = dataUnitsPerBucket;
    while (nextSplitPoint <= cmlDataUnits) {
        nextSplitPoint += dataUnitsPerBucket;
    }
    for (auto& p : sortedGatheredPivots) {
        uint8_t tag = p.second;
        switch (tag) {
            case BEGIN_PIVOT:
                numActiveSources++;
                break;
            case NORMAL_PIVOT:
                numNonBeginPivotsPassed++;
                break;
            case END_PIVOT:
                numActiveSources--;
                numNonBeginPivotsPassed++;
                break;
            default:
                assert(false);
        }

        cmlDataUnits = numNonBeginPivotsPassed * 2 +
                std::max(0, numActiveSources - 1);
        if (cmlDataUnits >= nextSplitPoint) {
            splitters.push_back(p.first);
            nextSplitPoint += dataUnitsPerBucket;
        }
    }
#else
    // Naive splitter selection algorithm: adapted from partition
    int totalPivots = numPivotsPerServer * world->size();
    int s = totalPivots / world->size();
    int k = world->size() * (s + 1) - totalPivots;
    int globalIdx = -1;
    for (int i = 0; i < world->size(); i++) {
        if (i < k) {
            globalIdx += s;
        } else {
            globalIdx += s + 1;
        }

        int localIdx = globalIdx - numPivotsPassed;
        if ((0 <= localIdx) && (localIdx < (int) sortedGatheredPivots.size())) {
            splitters.push_back(sortedGatheredPivots[localIdx].first);
        }
    }
#endif

    if (allPivotServers->rank + 1 == allPivotServers->size()) {
        if (!(splitters.back() == sortedGatheredPivots.back().first)) {
            LOG(ERROR, "The last splitter (%lu) is not the last pivot (%lu)?",
                    splitters.back().keyAsUint64(),
                    sortedGatheredPivots.back().first.keyAsUint64());
        }
    }
    timeTrace("pivot server picked %lu splitters locally", splitters.size());
    debugLogKeys("splitters (local portion): ", &splitters);

    // All-gather splitters between pivot servers.
    timeTrace("about to all-gather splitters");
    {
        PivotMerger merger(this, &splitters);
        ADD_COUNTER(allGatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(allGatherPivotsElapsedTime);
        Tub<AllGather> allGather;
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allPivotServers.get(),
                uint32_t(splitters.size() * PivotKey::SIZE),
                splitters.data(), &merger);
        allGather->wait();
        removeCollectiveOp(ALLGATHER_DATA_BUCKET_BOUNDARIES);
        ADD_COUNTER(allGatherPivotsMergeCycles, merger.activeCycles);
    }
    if (splitters.size() != world->size()) {
        DIE("Unexpected number of splitters, actual %lu, expected %d",
                splitters.size(), world->size());
    }
    timeTrace("all-gathered %lu splitters", splitters.size());

    // Sort the gathered splitters.
    std::sort(splitters.begin(), splitters.end());
    debugLogKeys("splitters: ", &splitters);

    // Broadcast splitters to slave nodes.
    timeTrace("about to broadcast %lu splitters to %d slave nodes",
            splitters.size(), myPivotServerGroup->size());
    broadcastSplitters->send<SendDataRpc>(
            BROADCAST_DATA_BUCKET_BOUNDARIES,
            uint32_t(PivotKey::SIZE * splitters.size()),
            splitters.data());
    // Note: there is no need to wait for the broadcast to finih as we are not
    // going to modify #splitters anymore.
//    broadcastSplitters->wait();

    // Pivot servers can advance to the final data shuffle immediately while
    // other nodes will have to wait until they receive the data bucket
    // boundaries via broadcast.
    shuffleRecords();
}

void
MilliSortService::shuffleRecords() {
    timeTrace("=== Stage 6: shuffleRecords started");

    // Partition the local keys and values according to the data bucket ranges;
    // this will be used to service the ShufflePull handler.
    // TODO: parallelize this step across all cores
    double keysPerPartition = double(numDataTuples) / double(splitters.size());
    const int step = std::max(1, int(std::sqrt(keysPerPartition)));
    int srcEnd = 0;
    for (const PivotKey& splitter : splitters) {
        int srcBegin = srcEnd;
        while ((srcEnd < numDataTuples) &&
                (localKeys[srcEnd] <= splitter)) {
            srcEnd = std::min(srcEnd + step, numDataTuples);
        }
        while ((srcEnd > srcBegin) &&
                !(localKeys[srcEnd-1] <= splitter)) {
            srcEnd--;
        }
        dataBucketRanges.emplace_back(srcBegin, srcEnd - srcBegin);
    }
    ADD_COUNTER(partitionElapsedTime, Cycles::rdtsc() - partitionStartTime);
    timeTrace("partitioned keys to be ready for key shuffle");

    // Local value rearrangement must be completed before shuffling values.
    uint64_t startWait = Cycles::rdtsc();
    uint64_t elapsedTime = rearrangeLocalVals->wait();
    if (rearrangeLocalVals->stopTime < startWait) {
        uint64_t slackTime = startWait - rearrangeLocalVals->stopTime;
        timeTrace("rearrangeLocalVals completed %u us ago, elapsed %u us",
                Cycles::toMicroseconds(slackTime),
                Cycles::toMicroseconds(elapsedTime));
    } else {
        uint64_t delayTime = Cycles::rdtsc() - startWait;
        timeTrace("rearrangeLocalVals completed %u us late, elapsed %u us",
                Cycles::toMicroseconds(delayTime),
                Cycles::toMicroseconds(elapsedTime));
    }
    ADD_COUNTER(rearrangeInitValuesElapsedTime, elapsedTime)

    ADD_COUNTER(shuffleKeysStartTime, Cycles::rdtsc() - startTime);
    START_TIMER(shuffleKeysElapsedTime);
#if !TWO_LEVEL_SHUFFLE
    // Push records to the right node they belong to (records that should
    // remain in the local node are also sent in the same way to simplify code).
    timeTrace("about to shuffle records");
    for (int rank = 0; rank < world->size(); rank++) {
        int keyIdx, numKeys;
        std::tie(keyIdx, numKeys) = dataBucketRanges[rank];
        Buffer* message = &recordShuffleMessages->at(rank);
        message->append(&localKeys[keyIdx], numKeys * PivotKey::SIZE);
        message->append(&localValues[keyIdx], numKeys * Value::SIZE);
        ADD_COUNTER(shuffleKeysOutputBytes, message->size());
    }
    recordShuffleIsReady = true;
    invokeShufflePush(world.get(), ALLSHUFFLE_RECORD,
            recordShuffleMessages.get());

    // Wait until we have received all keys that belong to us.
    while (recordShuffleRxCount.load() < world->size()) {
        Arachne::yield();
    }
#else
    // Push records to the right node they belong to (records that should
    // remain in the local node are also sent in the same way to simplify code).
    timeTrace("about to shuffle records within row group, size %d",
            twoLevelShuffleRowGroup->size());
    // Each iteration of the following loop assembles one "row-shuffle" message;
    // each of which aggregates `nrows` actual shuffle messages.
    int ncols = twoLevelShuffleRowGroup->size();
    for (int target = 0; target < ncols; target++) {
        // Each iteration of the following loop adds one actual message into
        // the aggregated "row-shuffle" message, with a preceding 32-bit
        // unsigned int describing its length.
        Buffer* message = &rowShuffleMessages->at(target);
        for (int rank = target; rank < world->size(); rank += ncols) {
            int keyIdx, numKeys;
            std::tie(keyIdx, numKeys) = dataBucketRanges[rank];
            uint32_t msgBytes = (PivotKey::SIZE + Value::SIZE) * numKeys;
            message->appendCopy(&msgBytes);
            message->append(&localKeys[keyIdx], numKeys * PivotKey::SIZE);
            message->append(&localValues[keyIdx], numKeys * Value::SIZE);
        }
        ADD_COUNTER(shuffleKeysOutputBytes, message->size());
    }
    rowShuffleIsReady = true;
    invokeShufflePush(twoLevelShuffleRowGroup.get(), ALLSHUFFLE_RECORD_ROW,
            rowShuffleMessages.get());

    // Wait until we have received all keys that belong to us.
    while (rowShuffleRxCount.load() < twoLevelShuffleRowGroup->size()) {
        Arachne::yield();
    }

    timeTrace("about to shuffle records within column group, size %d",
            twoLevelShuffleColGroup->size());
    colShuffleIsReady = true;
    invokeShufflePush(twoLevelShuffleColGroup.get(), ALLSHUFFLE_RECORD_COL,
            colShuffleMessages.get());

    // Wait until we have received all keys that belong to us.
    while (colShuffleRxCount.load() < twoLevelShuffleColGroup->size()) {
        Arachne::yield();
    }
#endif
    numSortedItems = numRecordsReceived;

    STOP_TIMER(shuffleKeysElapsedTime);
    ADD_COUNTER(millisortFinalPivotKeyBytes, numSortedItems * PivotKey::SIZE);
    ADD_COUNTER(millisortFinalValueBytes, numSortedItems * Value::SIZE);
    timeTrace("shuffle records completed, received %d records", numSortedItems);

    // Wait until the merge-sort is completed, then proceed to rearrange values.
    mergeSortKeys();
    rearrangeFinalVals();
}

void
MilliSortService::mergeSortKeys()
{
    static const int DIV_FACTOR = 20;
    static const int K_WAY = 4;

    ADD_COUNTER(onlineMergeSortStartTime, Cycles::rdtsc() - startTime)
    SCOPED_TIMER(onlineMergeSortElapsedTime)
#if ENABLE_MERGE_SORTER
    ADD_COUNTER(onlineMergeSortWorkers, coresAvail.size());
//        const uint32_t MERGE_CUT_OFF = 10;
    const uint32_t MERGE_CUT_OFF = std::max(2000u, downCast<uint32_t>(
            numRecordsReceived.load() / coresAvail.size() / DIV_FACTOR));
    timeTrace("mergesort started, MERGE_CUT_OFF %u, K_WAY %d",
            MERGE_CUT_OFF, K_WAY);

    std::sort(mergeSortChunks.begin(), mergeSortChunks.end());
    mergeSortChunks.push_back(incomingKeys + numRecordsReceived.load());
    PivotKey* dest = mergeSortTmpBuffer.get();

    // Temporary variables used in the following while-loop. Defined here
    // to pay the allocation cost once.
    // - topTasks:      top-level tasks created directly from pre-sorted chunks
    // - atomicTasks:   smaller tasks generated by splitting #topTasks
    // - outputQueues:  per-core output queues used by split workers to store
    //                  their generated atomic tasks
    // - resultChunks:  larger pre-sorted chunks after merge; used to update
    //                  #mergeSortChunks at the end of each iteration
    // - workers:       Arachne thread IDs of the parallel workers
    using MergeTask = MergeJob<PivotKey, K_WAY>;
    using MergeTaskQueue = std::vector<MergeTask>;
    std::vector<MergeTask> topTasks;
    std::vector<MergeTaskQueue> outputQueues(coresAvail.back() + 1);
    MergeTaskQueue* atomicTasks = &outputQueues.at(Arachne::core.id);
    std::vector<PivotKey*> resultChunks;
    std::vector<Arachne::ThreadId> workers;

    // Reserve enough storage to avoid dynamic (re-)allocation later.
    topTasks.reserve(mergeSortChunks.size() / K_WAY + 1);
    atomicTasks->reserve(numRecordsReceived.load() * 2 / MERGE_CUT_OFF);
    resultChunks.reserve(mergeSortChunks.size() / K_WAY);
    workers.reserve(coresAvail.size());

    // Each iteration of the following loop roughly reduces # sorted chunks
    // by a factor of K_WAY.
    uint32_t iter = 0;
    while (mergeSortChunks.size() > 2) {
        timeTrace("mergesort: start of iter %u, chunks left %u", iter,
                mergeSortChunks.size() - 1);
        iter++;
        debugLogKeys("itermediate result", numRecordsReceived,
                mergeSortChunks.front());
        uint64_t splitElapsed = Cycles::rdtsc();

        // In order to make the following processing easier, append dummy
        // chunks to make # chunks to merge is a multiple of K_WAY.
        while (mergeSortChunks.size() % K_WAY != 1) {
            mergeSortChunks.push_back(mergeSortChunks.back());
        }

        // Reset all temp variables that are reused across iterations.
        topTasks.clear();
        for (auto& out : outputQueues) {
            out.clear();
        }
        resultChunks.clear();
        workers.clear();

        // Create top-level K-way merge tasks; one in each iteration.
        for (uint32_t i = 0; i < mergeSortChunks.size() - 1; i += K_WAY) {
            topTasks.emplace_back();
            MergeTask& task = topTasks.back();
            for (uint32_t k = 0; k < K_WAY; k++) {
                task.srcBegin[k] = mergeSortChunks[i+k];
                task.srcEnd[k] = mergeSortChunks[i+k+1];
            }
            task.dest = dest;
            resultChunks.push_back(dest);
            dest += (task.srcEnd[K_WAY-1] - task.srcBegin[0]);
        }
        resultChunks.push_back(dest);
        dest = (resultChunks.front() == incomingKeys) ?
                mergeSortTmpBuffer.get() : incomingKeys;
        mergeSortChunks = resultChunks;

        // Spawn parallel split workers, wait for them to finish, and collect
        // generated atomic tasks.
        auto splitMain = [&topTasks, &outputQueues]
                (uint16_t MERGE_CUT_OFF, uint8_t totalWorkers, uint8_t id) {
            for (size_t i = id; i < topTasks.size(); i += totalWorkers) {
                parallelSplit(&topTasks[i], outputQueues.data(), MERGE_CUT_OFF);
            }
        };
        uint8_t totalWorkers = downCast<uint8_t>(
                std::min(coresAvail.size(), topTasks.size()));
        for (uint8_t i = 0; i < totalWorkers; i++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[i],
                    splitMain, MERGE_CUT_OFF, totalWorkers, i));
        }
        for (auto& tid : workers) {
            Arachne::join(tid);
        }
        for (auto& output : outputQueues) {
            if (&output != atomicTasks) {
                atomicTasks->insert(atomicTasks->end(), output.begin(),
                        output.end());
            }
        }
        splitElapsed = Cycles::rdtsc() - splitElapsed;
        timeTrace("mergesort: generated %u atomic tasks in %u us",
                atomicTasks->size(), Cycles::toMicroseconds(splitElapsed));

        // Parallelize the atomic tasks evenly among cores using a pull model
        // with a central queue.
        std::atomic<int> nextTask(0);
        auto workerMain = [&atomicTasks, &nextTask] {
            int totalTasks = 0;
            int totalKeys = 0;
            while (true) {
                int taskId = nextTask.fetch_add(1);
                if (taskId >= atomicTasks->size()) {
                    break;
                }
                MergeTask* task = &(*atomicTasks)[taskId];
                totalTasks++;
                for (int i = 0; i < K_WAY; i++) {
                    totalKeys += (task->srcEnd[i] - task->srcBegin[i]);
                }
                serialMerge(task);
            }
            timeTrace("mergesort: serial-merge worker finished, "
                    "tasks %d, keys %d", totalTasks, totalKeys);
        };

        workers.clear();
        for (int coreId : coresAvail) {
            workers.push_back(Arachne::createThreadOnCore(coreId,
                    workerMain));
        }
        for (auto& tid : workers) {
            Arachne::join(tid);
        }

    }
    debugLogKeys("merge-sort:", mergeSortChunks[1] - mergeSortChunks[0],
            mergeSortChunks[0]);
    sortedKeys = (mergeSortChunks.front() == incomingKeys) ?
            incomingKeys : mergeSortTmpBuffer.get();
#else
    // FIXME: figure out why using ips4o seems to cause stragglers in the
    // record shuffle above? My current suspicion is that the worker threads
    // in ips4o::ArachneThreadPool are not designed with collaborative
    // threading in mind so the ofiud worker threads can't make any progress
    int numWorkers = coresAvail.size();
    ADD_COUNTER(onlineMergeSortWorkers, numWorkers);
    ips4o::parallel::sort(incomingKeys, incomingKeys + numSortedItems,
            std::less<PivotKey>(), numWorkers);
    sortedKeys = incomingKeys;
    timeTrace("sorted %u final keys", numSortedItems);
#endif
}

void
MilliSortService::rearrangeFinalVals()
{
    timeTrace("about to rearrange final values");
    RearrangeValueTask rearrangeFinalVals(sortedKeys, incomingValues,
            sortedValues);
    rearrangeValues(&rearrangeFinalVals, numSortedItems, false);
    ADD_COUNTER(rearrangeFinalValuesElapsedTime, rearrangeFinalVals.wait())

    ADD_COUNTER(millisortTime, Cycles::rdtsc() - startTime);
    ADD_COUNTER(millisortFinalItems, numSortedItems);
    timeTrace("millisort finished, numSortedItems %d", numSortedItems);

    // Clean up async. broadcast operations.
    if (bcastPivotBucketBoundaries) {
        bcastPivotBucketBoundaries->wait();
    }
    if (broadcastSplitters) {
        broadcastSplitters->wait();
    }

    printingResult = true;
    ongoingMilliSort.load()->sendReply();
    ongoingMilliSort = NULL;

#if VALIDATE_RESULT
    uint64_t prevKey = 0;
    bool success = true;
    for (int i = 0; i < numSortedItems; i++) {
        // TODO: how to check if keys are globally sorted?
        uint64_t key = sortedKeys[i].keyAsUint64();
        if (prevKey > key) {
            LOG(ERROR, "Validation failed: unordered keys, index %d", i);
            success = false;
            break;
        }
        if (key != sortedValues[i].asUint64()) {
            LOG(ERROR, "Validation failed: unequal key and value, index %d", i);
            success = false;
            break;
        }
        prevKey = key;
    }
    if (success) {
        LOG(NOTICE, "Validation passed");
    }
#endif

#if LOG_FINAL_RESULT
    string result;
    for (int i = 0; i < numSortedItems; i++) {
        result += std::to_string(sortedKeys[i].keyAsUint64());
        result += ":";
        result += std::to_string(sortedValues[i].asUint64());
        result += " ";
        if (i % 100 == 0) {
            Arachne::yield();
        }
    }
    Arachne::yield();
    LOG(NOTICE, "final result (%d items): %s", numSortedItems, result.c_str());
#endif
    printingResult = false;
}

void
MilliSortService::debugLogKeys(const char* prefix, vector<PivotKey>* keys)
{
    debugLogKeys(prefix, keys->size(), keys->data());
}

void
MilliSortService::debugLogKeys(const char* prefix, int numKeys, PivotKey* keys)
{
#if LOG_INTERMEDIATE_RESULT
    string result;
    uint64_t checksum = 0;
    uint64_t index = 0;
    for (int i = 0; i < numKeys; i++) {
        PivotKey* sortKey = keys + i;
        result += std::to_string(sortKey->keyAsUint64());
        result += " ";
        index++;
        checksum += sortKey->keyAsUint64() * index;
    }
    LOG(NOTICE, "%s (csum = %s) %s", prefix, std::to_string(checksum).c_str(),
            result.c_str());
#endif
}

void
MilliSortService::treeBcast(const WireFormat::TreeBcast::Request* reqHdr,
            WireFormat::TreeBcast::Response* respHdr, Rpc* rpc)
{
    TreeBcast::handleRpc(context, reqHdr, rpc);
}

void
MilliSortService::treeGather(const WireFormat::TreeGather::Request* reqHdr,
            WireFormat::TreeGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<TreeGather>(reqHdr, respHdr, rpc);
}

void
MilliSortService::allGather(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<AllGather>(reqHdr, respHdr, rpc);
}

// TODO: doc
void
MilliSortService::copyOutShufflePivots(uint32_t senderId, uint32_t totalLength,
        uint32_t offset, Buffer* messageChunk)
{
    if (totalLength != messageChunk->size()) {
        LOG(ERROR, "totalLength %u, offset %u, len %u", totalLength, offset,
                messageChunk->size());
        DIE("Caveat: pivot shuffle message must fit in one chunk!");
    }

    // TODO: the code below is serialized and far from optimal;
    static SpinLock mutex("pivotShuffle::mutex");
    SpinLock::Guard lock(mutex);
    SCOPED_TIMER(mergePivotsInBucketSortCycles);

    uint32_t readOffset = 0;
    uint32_t numSmallerPivots = *messageChunk->read<uint32_t>(&readOffset);
    uint32_t numSmallerBeginPivots = *messageChunk->read<uint32_t>(&readOffset);
    uint32_t numSmallerEndPivots = *messageChunk->read<uint32_t>(&readOffset);
    uint32_t numSlaves = *messageChunk->read<uint32_t>(&readOffset);
    int32_t* beginPivots = messageChunk->getOffset<int32_t>(readOffset);
    readOffset += numSlaves * sizeof(int32_t);
    int32_t* endPivots = messageChunk->getOffset<int32_t>(readOffset);
    readOffset += numSlaves * sizeof(int32_t);

    size_t oldSize = sortedGatheredPivots.size();
    while (readOffset < messageChunk->size()) {
        PivotKey* pivot = messageChunk->read<PivotKey>(&readOffset);
        sortedGatheredPivots.emplace_back(*pivot, NORMAL_PIVOT);
    }
    assert(readOffset == messageChunk->size());
    uint32_t newPivots = sortedGatheredPivots.size() - oldSize;
    for (uint32_t i = 0; i < numSlaves; i++) {
        uint32_t idx = beginPivots[i] - numSmallerPivots;
        if (idx < newPivots) {
            sortedGatheredPivots[oldSize + idx].second = BEGIN_PIVOT;
        }
        idx = endPivots[i] - numSmallerPivots;
        if (idx < newPivots) {
            sortedGatheredPivots[oldSize + idx].second = END_PIVOT;
        }
    }
    numActiveSources += numSmallerBeginPivots - numSmallerEndPivots;
    numNonBeginPivotsPassed += numSmallerPivots - numSmallerBeginPivots;
    numPivotsPassed += numSmallerPivots;
    timeTrace("numActiveSources %u, numNonBeginPivotsPassed %u, "
            "numPivotsPassed %u", numActiveSources, numNonBeginPivotsPassed,
            numPivotsPassed);

    std::inplace_merge(sortedGatheredPivots.begin(),
            sortedGatheredPivots.begin() + oldSize,
            sortedGatheredPivots.end());
    timeTrace("inplace-merged %lu and %lu pivots", oldSize,
            sortedGatheredPivots.size() - oldSize);

    pivotShuffleRxCount.fetch_add(1);
}

/**
 * Incorporate incoming records received in the record shuffle phase.
 *
 * \param senderId
 *      Rank of the server where the records are sent.
 * \param totalLength
 *      Length of the complete shuffle message (not chunk), in bytes.
 * \param offset
 *      Offset, in bytes, of this chunk in the complete message.
 * \param messageChunk
 *      Buffer holding the incoming records.
 */
void
MilliSortService::copyOutShuffleRecords(uint32_t senderId, uint32_t totalLength,
        uint32_t offset, Buffer* messageChunk)
{
    // Special case: not a real chunk; this is a side-effect of piggybacking
    // shuffle message chunks in RPC responses.
    if (offset > totalLength) {
        return;
    }

    assert(totalLength % (PivotKey::SIZE + Value::SIZE) == 0);
    uint32_t numRecords = totalLength / (PivotKey::SIZE + Value::SIZE);
    uint32_t len = messageChunk->size();
    assert(offset + len <= totalLength);

    // Reserve space for the entire message if this is the first chunk of the
    // message. Note: the caller of this method should've filtered out duplicate
    // RPCs, or message chunks, already; otherwise, we may risk double-counting
    // incoming records.
    std::atomic<int>* resv = &shuffleRecordResv->at(senderId);
    if (offset == 0) {
        assert(resv->load() == -1);
        resv->store(numRecordsReceived.fetch_add(numRecords));
    } else {
        while (resv->load() < 0) {
            Arachne::yield();
        }
    }

    // Copy incoming keys, if any, to their destination.
    const uint32_t startIdx = downCast<uint32_t>(resv->load());
    uint32_t keyOffsetEnd = numRecords * PivotKey::SIZE;
    uint32_t keyBytesInMessage = 0;
    if (offset < keyOffsetEnd) {
        Buffer::Iterator payload(messageChunk, 0, keyOffsetEnd - offset);
        keyBytesInMessage = payload.size();
        char* dst = reinterpret_cast<char*>(&incomingKeys[startIdx]);
        dst += offset;
        while (!payload.isDone()) {
            SCOPED_TIMER(shuffleKeysCopyResponseCycles);
            memcpy(dst, payload.getData(), payload.getLength());
            dst += payload.getLength();
            payload.next();
        }
    }

    // Copy incoming values, if any, to their destination.
    if (offset + len > keyOffsetEnd) {
        Buffer::Iterator payload(messageChunk, keyBytesInMessage, ~0u);
        char* dst = reinterpret_cast<char*>(&incomingValues[startIdx]);
        if (offset >= keyOffsetEnd) {
            dst += offset - keyOffsetEnd;
        }
        while (!payload.isDone()) {
            SCOPED_TIMER(shuffleKeysCopyResponseCycles);
            memcpy(dst, payload.getData(), payload.getLength());
            dst += payload.getLength();
            payload.next();
        }
    }

    // This is the last chunk of the message, which implies that we
    // have received the entire message because our push-based shuffle
    // implementation doesn't send out the next data chunk before the
    // preceding one is finished.
    if (offset + len == totalLength) {
        // Note: there is no need to worry about the message being added to
        // the merge sorter twice before we have filtered out duplicate RPCs
        // before calling this method.
        timeTrace("received %u records from server %u", numRecords,
                senderId);

        // Fix the value indices of incoming keys.
        for (uint32_t i = startIdx; i < startIdx + numRecords; i++) {
            incomingKeys[i].index = i;
        }

#if ENABLE_MERGE_SORTER
        // Notify the merge sorter about the incoming keys.
        mergeSorterLock.lock();
        mergeSortChunks.push_back(&incomingKeys[startIdx]);
        mergeSorterLock.unlock();
#endif
        recordShuffleRxCount.fetch_add(1);
        ADD_COUNTER(shuffleKeysInputBytes, totalLength);
    }
}

void
MilliSortService::ShuffleOp::onReceive(
        const WireFormat::ShufflePush::Request* reqHdr,
        WireFormat::ShufflePush::Response* respHdr,
        Buffer* request, Buffer* reply)
{
    const uint32_t totalLength = reqHdr->totalLength;
    const uint32_t offset = reqHdr->offset;
    const uint32_t chunkSize = reqHdr->chunkSize;
    const uint32_t senderId = downCast<uint32_t>(reqHdr->senderId);

    timeTraceSp("shuffle: received message chunk from server %u, offset %u, "
            "len %u", senderId, offset, request->size());
    std::atomic<uint32_t>* bytesReceived = &rxBytes[senderId];
    if (request->size() > 0) {
        // Serialize the processing of incoming message chunks that
        // belong to the same message to prevent duplicate RPCs from
        // corrupting #incomingMessages.
        while (bytesReceived->load() < offset) {
            Arachne::yield();
        }
        uint32_t expected = offset;
        if (bytesReceived->compare_exchange_strong(expected,
                offset + request->size())) {
            copyOut(senderId, totalLength, offset, request);
        } else {
            LOG(WARNING, "ShuffleOp %d: ignore duplicate message "
                    "chunk from senderId %u, offset %u", opId, senderId,
                    offset);
        }
    } else if (offset == 0) {
        uint32_t expected = offset;
        if (bytesReceived->compare_exchange_strong(expected, 1)) {
            copyOut(senderId, totalLength, offset, request);
        } else {
            LOG(WARNING, "ShuffleOp %d: ignore duplicate message "
                    "chunk from senderId %u, offset %u", opId, senderId,
                    offset);
        }
    }

    // Piggyback a message chunk, if any, in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
    if (senderId == group->rank) return;
    respHdr->totalLength = outgoingMessages[senderId].size();
    respHdr->offset = offset;
    reply->appendExternal(&outgoingMessages[senderId], offset, chunkSize);
#endif
}

char*
MilliSortService::ShuffleOp::getCopyDest(uint32_t senderId,
        uint32_t totalLength, uint32_t offset)
{
    // Note: the caller of this method should've filtered out duplicate RPCs,
    // or message chunks, already; otherwise, there will be a data race.
    if (offset == 0) {
        incomingMessages[senderId].resize(totalLength, 0);
        bufferInited[senderId] = true;
    } else {
        while (!bufferInited[senderId]) {
            Arachne::yield();
        }
    }
    return incomingMessages[senderId].data() + offset;
}

void
MilliSortService::ShuffleOp::copyOut(uint32_t senderId, uint32_t totalLength,
        uint32_t offset, Buffer* messageChunk)
{
    // Special case: not a real chunk; this is a side-effect of piggybacking
    // shuffle message chunks in RPC responses.
    if (offset > totalLength) {
        return;
    }

    uint32_t len = messageChunk->size();
    assert(offset + len <= totalLength);

    // Copy incoming message bytes to their destination.
    if (offset < totalLength) {
        Buffer::Iterator payload(messageChunk);
        char* dst = getCopyDest(senderId, totalLength, offset);
        while (!payload.isDone()) {
            memcpy(dst, payload.getData(), payload.getLength());
            dst += payload.getLength();
            payload.next();
        }
    }

    // This is the last chunk of the message, which implies that we
    // have received the entire message because our push-based shuffle
    // implementation doesn't send out the next data chunk before the
    // preceding one is finished.
    if (offset + len == totalLength) {
        rxCompleted.fetch_add(1);
        totalRxBytes.fetch_add(totalLength);
    }
}

void
MilliSortService::handleShuffleRowChunk(uint32_t senderId, uint32_t totalLength,
        uint32_t offset, Buffer* messageChunk)
{
    // Special case: not a real chunk; this is a side-effect of piggybacking
    // shuffle message chunks in RPC responses.
    if (offset > totalLength) {
        return;
    }

    uint32_t len = messageChunk->size();
    if (offset + len < totalLength) {
        // TODO: fix this limitation? we need at least 3 more things:
        // 1) record # bytes in the last incomplete msg
        // 2) a temp buffer to hold the last incomplete msg
        // 2) serialize the processing of msg chunks
        DIE("row message must fit in one chunk for now!");
    }

    // Each iteration of the following loop slices out one shuffle message and
    // redirects it to the right out buffer.
    uint32_t curOffset = 0;
    for (int i = 0; i < twoLevelShuffleColGroup->size(); i++) {
        uint32_t msgBytes = *messageChunk->getOffset<uint32_t>(curOffset);
        curOffset += sizeof32(msgBytes);
        SpinLock::Guard _(colShuffleMessageLocks->at(i));
        colShuffleMessages->at(i).appendCopy(&msgBytes);
        colShuffleMessages->at(i).appendCopy(messageChunk, curOffset, msgBytes);
        curOffset += msgBytes;
    }

    if (offset + len == totalLength) {
        rowShuffleRxCount.fetch_add(1);
        ADD_COUNTER(shuffleKeysInputBytes, totalLength);
        ADD_COUNTER(shuffleKeysOutputBytes, totalLength);
    }
}

void
MilliSortService::handleShuffleColChunk(uint32_t senderId, uint32_t totalLength,
        uint32_t offset, Buffer* messageChunk)
{
    // Special case: not a real chunk; this is a side-effect of piggybacking
    // shuffle message chunks in RPC responses.
    if (offset > totalLength) {
        return;
    }

    int ncols = twoLevelShuffleRowGroup->size();
    uint32_t dataBytes = totalLength - sizeof32(uint32_t) * ncols;
    assert(dataBytes % (PivotKey::SIZE + Value::SIZE) == 0);
    uint32_t len = messageChunk->size();
    assert(offset + len <= totalLength);

    if (offset + len < totalLength) {
        // TODO: fix this limitation?
        DIE("col message must fit in one chunk for now!");
    }

    // Each iteration of the following loop processes one original shuffle
    // message.
    uint32_t curOffset = 0;
    for (int i = 0; i < ncols; i++) {
        uint32_t msgBytes = *messageChunk->getOffset<uint32_t>(curOffset);
        curOffset += sizeof32(msgBytes);

        uint32_t realSenderId = senderId * ncols + i;
        std::atomic<int>* resv = &shuffleRecordResv->at(realSenderId);
        int numRecords = msgBytes / (PivotKey::SIZE + Value::SIZE);
        resv->store(numRecordsReceived.fetch_add(numRecords));

        // Copy incoming keys to their destination.
        const uint32_t startIdx = downCast<uint32_t>(resv->load());
        uint32_t keyBytes = numRecords * PivotKey::SIZE;
        char* dst = reinterpret_cast<char*>(&incomingKeys[startIdx]);
        memcpy(dst, messageChunk->getRange(curOffset, keyBytes), keyBytes);
        curOffset += keyBytes;

        // Copy incoming values to their destination.
        uint32_t valueBytes = numRecords * Value::SIZE;
        dst = reinterpret_cast<char*>(&incomingValues[startIdx]);
        memcpy(dst, messageChunk->getRange(curOffset, valueBytes), valueBytes);
        curOffset += valueBytes;

        // Fix the value indices of incoming keys.
        for (uint32_t i = startIdx; i < startIdx + numRecords; i++) {
            incomingKeys[i].index = i;
        }

#if ENABLE_MERGE_SORTER
        // Notify the merge sorter about the incoming keys.
        mergeSorterLock.lock();
        mergeSortChunks.push_back(&incomingKeys[startIdx]);
        mergeSorterLock.unlock();
#endif
    }

    if (offset + len == totalLength) {
        colShuffleRxCount.fetch_add(1);
        ADD_COUNTER(shuffleKeysInputBytes, totalLength);
    }
}

/**
 *
 * \param group
 * \param dataId
 * \param outMessages
 *      Shuffle messages to be sent to all the other nodes in the communication
 *      group, in clockwise order starting from the local node.
 */
void
MilliSortService::invokeShufflePush(CommunicationGroup* group, uint32_t dataId,
        std::vector<Buffer>* outMessages, ShuffleOp* shuffleOp)
{
    assert(group->size() == outMessages->size());
    struct ShuffleMessageTracker {
        /// Contains the ongoing RPC used to send a chunk of this message.
        Tub<ShufflePushRpc> outstandingRpc;

        /// Rank of the message receiver within the communication group of the
        /// shuffle operation.
        uint32_t rank;

        /// ServerId of the message receiver. Used for logging only.
        uint32_t receiverId;

        /// Session to the receiver.
        Transport::SessionRef session;

        /// Size of the complete message, in bytes.
        uint32_t transmitTotalLength;

        /// Offset of the first byte yet to be transmitted.
        uint32_t transmitOffset;

        /// Size of the complete message, in bytes.
        uint32_t receiveTotalLength;

        /// Offset of the first byte yet to be transmitted.
        uint32_t receiveOffset;

        /// Range of the message yet to be transmitted.
        Buffer::Iterator messageIt;

        /// Time, in rdtsc ticks, when we send out #outstandingRpc.
        uint64_t lastTransmitTime;

        ShuffleMessageTracker(uint32_t rank, ServerId serverId,
                Transport::SessionRef session, Buffer* message)
            : outstandingRpc()
            , rank(rank)
            , receiverId(serverId.indexNumber() - 1)
            , session(session)
            , transmitTotalLength(message->size())
            , transmitOffset(0)
            , receiveTotalLength(~0u)
            , receiveOffset(0)
            , messageIt(message)
            , lastTransmitTime()
        {}

        ~ShuffleMessageTracker() {}

        void
        sendMessageChunk(Context* context, int senderId, uint32_t dataId,
                uint32_t chunkSize)
        {
            uint32_t len = std::min(chunkSize, messageIt.size());
            timeTraceSp("shuffle: sending message chunk to server %u, "
                    "offset %u, len %u, chunkSize %u", receiverId,
                    transmitOffset, len, chunkSize);
            assert(!outstandingRpc);
            Buffer::Iterator payload(messageIt);
            payload.truncate(len);
            outstandingRpc.construct(context, session, senderId, dataId,
                    transmitTotalLength, transmitOffset, chunkSize, &payload);
            timeTraceSp("shuffle: push rpc constructed, rpcId %u",
                    outstandingRpc->rpcId);
            transmitOffset += chunkSize;
            messageIt.advance(len);
        }

        // FIXME: hack to implement piggyback message chunk in shuffle push reply.
        uint32_t getBytesLeft() {
            return messageIt.size() + (PIGGYBACK_SHUFFLE_MSG ?
                    (receiveTotalLength - receiveOffset) : 0);
        }

        bool isDone() {
            return messageIt.isDone() && (PIGGYBACK_SHUFFLE_MSG ?
                    (receiveOffset == receiveTotalLength) : true);
        }
    };

    uint64_t activeCycles = context->dispatch->dispatchActiveCycles;
    uint64_t totalBytesSent = 0;

#if !PIGGYBACK_SHUFFLE_MSG
    int numOutMessages = group->size();
#else
    int numOutMessages = (group->size() + 1) / 2;
    if (group->size() % 2 == 0) {
        if (numOutMessages % 2) {
            numOutMessages += group->rank % 2 ? 0 : 1;
        } else {
            numOutMessages += (group->rank < numOutMessages) ? 1 : 0;
        }
    }
#endif

    std::unique_ptr<Tub<ShuffleMessageTracker>[]> messageTrackers(
            new Tub<ShuffleMessageTracker>[numOutMessages]);
    for (int i = 0; i < numOutMessages; i++) {
        uint32_t remoteRank = (group->rank + i) % group->size();
        Buffer* message = &(*outMessages)[remoteRank];
        ServerId serverId = group->getNode(remoteRank);
        messageTrackers[i].construct(remoteRank, serverId,
                sessions[serverId.indexNumber()-1], message);
        totalBytesSent += message->size();
    }

    // Performance stats.
    uint64_t shuffleStart = Cycles::rdtsc();
    uint32_t outstandingChunks = 0;

    std::vector<int> candidates;
    int messagesUnfinished = numOutMessages;
    while (messagesUnfinished > 0) {
        // TODO: we might want to relax the "no outstanding RPC" restrict to
        // allow multiple chunks from the same message in flight; otherwise,
        // we might be limited by the network latency when we have only a few
        // messages left?

        candidates.clear();

        // FIXME: hack? perform our own app-level rate control
        int transmitPendingRpcs = 0;

        // Each iteration of the following loop does one of the two things for
        // each message: (1) if there is no outstanding chunk being sent, check
        // for RPC completion; (2) otherwise, find the message with maximum
        // remaining bytes.
        for (int i = 0; i < numOutMessages; i++) {
            // Check RPC completion.
            ShuffleMessageTracker* messageTracker = messageTrackers[i].get();
            ShufflePushRpc* rpc = messageTracker->outstandingRpc.get();
            if (rpc) {
                transmitPendingRpcs += rpc->transmitDone ? 0 : 1;
                if (rpc->isReady()) {
                    outstandingChunks--;
                    timeTraceSp("shuffle: message chunk to server %u acked,"
                            " offset %u, RTT %u us, outstanding chunks %u",
                            messageTracker->receiverId,
                            messageTracker->transmitOffset,
                            Cycles::toMicroseconds(Cycles::rdtsc() -
                            messageTracker->lastTransmitTime),
                            outstandingChunks);

                    // Copy out records piggybacked in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
                    Buffer* reply = rpc->wait();
                    WireFormat::ShufflePush::Response* respHdr = reply->
                            getStart<WireFormat::ShufflePush::Response>();
                    messageTracker->receiveTotalLength = respHdr->totalLength;
                    assert(messageTracker->receiveOffset ==
                            std::min(respHdr->offset, respHdr->totalLength));
                    const uint32_t respOffset = respHdr->offset;
                    if (respHdr->offset <= respHdr->totalLength) {
                        timeTraceSp("shuffle: found piggybacked message "
                                "chunk, offset %u, len %u, totalLength %u",
                                respHdr->offset, reply->size(),
                                respHdr->totalLength);
                    }
                    reply->truncateFront(sizeof32(*respHdr));
                    if (messageTracker->rank != group->rank) {
                        if (dataId == ALLSHUFFLE_PIVOTS) {
                            copyOutShufflePivots(messageTracker->rank,
                                    messageTracker->receiveTotalLength,
                                    respOffset, reply);
                        } else if (dataId == ALLSHUFFLE_RECORD) {
                            copyOutShuffleRecords(messageTracker->rank,
                                    messageTracker->receiveTotalLength,
                                    respOffset, reply);
                        } else if ((dataId >= ALLSHUFFLE_BIGQUERY_0) && (dataId < ALLSHUFFLE_BIGQUERY_END)) {
                            shuffleOp->copyOut(messageTracker->rank,
                                    messageTracker->receiveTotalLength,
                                    respOffset, reply);
                        } else if (dataId == ALLSHUFFLE_RECORD_ROW) {
                            handleShuffleRowChunk(messageTracker->rank,
                                    messageTracker->receiveTotalLength,
                                    respOffset, reply);
                        } else if (dataId == ALLSHUFFLE_RECORD_COL) {
                            handleShuffleColChunk(messageTracker->rank,
                                    messageTracker->receiveTotalLength,
                                    respOffset, reply);
                        }
                        messageTracker->receiveOffset += reply->size();
                    }
#endif
                    messageTracker->outstandingRpc.destroy();
                    if (messageTracker->isDone()) {
                        messagesUnfinished--;
                        timeTraceSp("shuffle: message to server %u done, "
                                "%d messages left", messageTracker->receiverId,
                                messagesUnfinished);
                    }
                } else {
                    // This message has an outstanding chunk being sent;
                    // do not consider it for transmission again until that
                    // chunk is completed.
                    continue;
                }
            }

            if (messageTrackers[i]->getBytesLeft() > 0) {
                candidates.push_back(i);
            }
        }

        // TODO: what should be a healthy amout of pending RPCs?
        const int maxTxPendingRpcs = 8;
        const uint32_t maxOutstandingChunks = 16;
        const int maxChoices = 2;

        // Power-of-two LRPT policy.
        int index = -1;
        uint32_t maxBytesLeft = 0;
        bool readyToSend = (outstandingChunks < maxOutstandingChunks) &&
                (transmitPendingRpcs < maxTxPendingRpcs);
        if (!candidates.empty() && readyToSend) {
            for (int choice = 0; choice < maxChoices; choice++) {
                int idx = candidates[Util::wyhash64() % candidates.size()];
                uint32_t bytesLeft = messageTrackers[idx]->getBytesLeft();
                if (bytesLeft > maxBytesLeft) {
                    maxBytesLeft = bytesLeft;
                    index = idx;
                }
            }
        }

        // Send out one more chunk of data, if appropriate.
        if (readyToSend && (index >= 0)) {
            uint64_t now = Cycles::rdtsc();
            messageTrackers[index]->lastTransmitTime = now;
            messageTrackers[index]->sendMessageChunk(context, group->rank,
                    dataId, MAX_SHUFFLE_CHUNK_SIZE);
            outstandingChunks++;

            if (dataId == ALLSHUFFLE_RECORD) {
                INC_COUNTER(shuffleKeysSentRpcs);
            } else if ((dataId == ALLSHUFFLE_RECORD_ROW) ||
                    (dataId == ALLSHUFFLE_RECORD_COL)) {
                INC_COUNTER(shuffleKeysSentRpcs);
            } else if (dataId == ALLSHUFFLE_PIVOTS) {
                // TODO
            }
        }

        Arachne::yield();
    }

    activeCycles = context->dispatch->dispatchActiveCycles - activeCycles;
    uint64_t elapsed = Cycles::rdtsc() - shuffleStart;
#if !PIGGYBACK_SHUFFLE_MSG
    timeTraceSp("shuffle: operation completed, idle %u us, active %u us, "
            "active BW %u Gbps, actual BW %u Gbps",
            Cycles::toMicroseconds(elapsed - activeCycles),
            Cycles::toMicroseconds(activeCycles),
            uint32_t(double(totalBytesSent)*8e-9/Cycles::toSeconds(activeCycles)),
            uint32_t(double(totalBytesSent)*8e-9/Cycles::toSeconds(elapsed)));
#endif

    if (shuffleOp) {
        while (!shuffleOp->isDone()) {
            Arachne::yield();
        }
    }
}

void
MilliSortService::shufflePush(const WireFormat::ShufflePush::Request* reqHdr,
            WireFormat::ShufflePush::Response* respHdr, Rpc* rpc)
{
    // Copy out fields from the request header and chop off the header.
    const uint32_t totalLength = reqHdr->totalLength;
    const uint32_t offset = reqHdr->offset;
    const uint32_t chunkSize = reqHdr->chunkSize;
    const uint32_t senderId = downCast<uint32_t>(reqHdr->senderId);
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    respHdr->receiverId = world->rank;

    switch (reqHdr->dataId) {
        case ALLSHUFFLE_PIVOTS: {
            uint32_t serverId =
                    allPivotServers->getNode(senderId).indexNumber() - 1;
            timeTraceSp("shuffle-pivot: received message chunk from server %u,"
                    " offset %u, len %u", serverId, offset,
                    rpc->requestPayload->size());
            std::atomic<uint32_t>* bytesReceived =
                    &pivotShuffleProgress->at(senderId);
            if (rpc->requestPayload->size() > 0) {
                // Serialize the processing of incoming message chunks that
                // belong to the same message to prevent duplicate RPCs.
                while (bytesReceived->load() < offset) {
                    Arachne::yield();
                }
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected,
                        offset + rpc->requestPayload->size())) {
                    copyOutShufflePivots(serverId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "PivotShuffle: ignore duplicate message chunk "
                            "from server %u, offset %u", serverId, offset);
                }
            } else if (offset == 0) {
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected, 1)) {
                    copyOutShufflePivots(senderId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "PivotShuffle: ignore duplicate message "
                            "chunk from senderId %u, offset %u", senderId,
                            offset);
                }
            }

            // Piggyback a message chunk, if any, in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
            if (senderId == allPivotServers->rank) break;
            while (!pivotShuffleIsReady.load()) {
                Arachne::yield();
            }

            respHdr->totalLength = pivotShuffleMessages->at(senderId).size();
            respHdr->offset = offset;
            rpc->replyPayload->appendExternal(
                    &pivotShuffleMessages->at(senderId), offset, chunkSize);
#endif
            break;
        }
        case ALLSHUFFLE_BIGQUERY_0:
        case ALLSHUFFLE_BIGQUERY_1:
        case ALLSHUFFLE_BIGQUERY_2:
        case ALLSHUFFLE_BIGQUERY_3: {
            auto& shuffleOp = globalShuffleOpTable[reqHdr->dataId];
            uint64_t deadline = Cycles::rdtsc() + Cycles::fromSeconds(10);
            while (shuffleOp.load() == NULL) {
                if (Cycles::rdtsc() > deadline) {
                    DIE("Couldn't find shuffleOp %u after 10s", reqHdr->dataId);
                }
                Arachne::yield();
            }
            shuffleOp.load()->onReceive(reqHdr, respHdr, rpc->requestPayload,
                    rpc->replyPayload);
            break;
        }
        case ALLSHUFFLE_RECORD: {
            timeTraceSp("shuffle-record: received message chunk from server %u,"
                    " offset %u, len %u", senderId, offset,
                    rpc->requestPayload->size());
            std::atomic<uint32_t>* bytesReceived =
                    &recordShuffleProgress->at(senderId);
            if (rpc->requestPayload->size() > 0) {
                // Serialize the processing of incoming message chunks that
                // belong to the same message to prevent duplicate RPCs from
                // corrupting #incomingKeys.
                while (bytesReceived->load() < offset) {
                    Arachne::yield();
                }
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected,
                        offset + rpc->requestPayload->size())) {
                    INC_COUNTER(shuffleKeysReceivedRpcs);
                    copyOutShuffleRecords(senderId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "RecordShuffle: ignore duplicate message "
                            "chunk from senderId %u, offset %u", senderId,
                            offset);
                }
            } else if (offset == 0) {
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected, 1)) {
                    copyOutShuffleRecords(senderId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "RecordShuffle: ignore duplicate message "
                            "chunk from senderId %u, offset %u", senderId,
                            offset);
                }
            }

            // Piggyback a message chunk, if any, in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
            if (senderId == world->rank) break;
            while (!recordShuffleIsReady.load()) {
                Arachne::yield();
            }

            respHdr->totalLength = recordShuffleMessages->at(senderId).size();
            respHdr->offset = offset;
            rpc->replyPayload->appendExternal(
                    &recordShuffleMessages->at(senderId), offset, chunkSize);
#endif
            break;
        }
        case ALLSHUFFLE_RECORD_ROW: {
            timeTraceSp("shuffle-row: received message chunk from server %u,"
                    " offset %u, len %u", senderId, offset,
                    rpc->requestPayload->size());
            if (rpc->requestPayload->size() > 0) {
                // Serialize the processing of incoming message chunks that
                // belong to the same message to prevent duplicate RPCs from
                // corrupting #incomingKeys.
                std::atomic<uint32_t>* bytesReceived =
                        &rowShuffleProgress->at(senderId);
                while (bytesReceived->load() < offset) {
                    Arachne::yield();
                }
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected,
                        offset + rpc->requestPayload->size())) {
                    INC_COUNTER(shuffleKeysReceivedRpcs);
                    handleShuffleRowChunk(senderId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "RowShuffle: ignore duplicate message "
                            "chunk from senderId %u, offset %u", senderId,
                            offset);
                }
            }

            // Piggyback a message chunk, if any, in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
            if (senderId == twoLevelShuffleRowGroup->rank) break;
            while (!rowShuffleIsReady.load()) {
                Arachne::yield();
            }

            respHdr->totalLength = rowShuffleMessages->at(senderId).size();
            respHdr->offset = offset;
            rpc->replyPayload->appendExternal(
                    &rowShuffleMessages->at(senderId), offset, chunkSize);
#endif
            break;
        }
        case ALLSHUFFLE_RECORD_COL: {
            timeTraceSp("shuffle-col: received message chunk from server %u,"
                    " offset %u, len %u", senderId, offset,
                    rpc->requestPayload->size());
            if (rpc->requestPayload->size() > 0) {
                // Serialize the processing of incoming message chunks that
                // belong to the same message to prevent duplicate RPCs from
                // corrupting #incomingKeys.
                std::atomic<uint32_t>* bytesReceived =
                        &colShuffleProgress->at(senderId);
                while (bytesReceived->load() < offset) {
                    Arachne::yield();
                }
                uint32_t expected = offset;
                if (bytesReceived->compare_exchange_strong(expected,
                        offset + rpc->requestPayload->size())) {
                    INC_COUNTER(shuffleKeysReceivedRpcs);
                    handleShuffleColChunk(senderId, totalLength, offset,
                            rpc->requestPayload);
                } else {
                    LOG(WARNING, "ColShuffle: ignore duplicate message "
                            "chunk from senderId %u, offset %u", senderId,
                            offset);
                }
            }

            // Piggyback a message chunk, if any, in the RPC response.
#if PIGGYBACK_SHUFFLE_MSG
            if (senderId == twoLevelShuffleColGroup->rank) break;
            while (!colShuffleIsReady.load()) {
                Arachne::yield();
            }

            respHdr->totalLength = colShuffleMessages->at(senderId).size();
            respHdr->offset = offset;
            rpc->replyPayload->appendExternal(
                    &colShuffleMessages->at(senderId), offset, chunkSize);
#endif
            break;
        }
        case ALLSHUFFLE_BENCHMARK: {
            // Do nothing; we are benchmarking the transport layer.
            timeTraceSp("shuffle-bench: received message chunk, senderId %d, "
                    "offset %u, len %u", senderId, offset,
                    rpc->requestPayload->size());
            if (offset + chunkSize >= totalLength) {
                timeTraceSp("shuffle-bench: received complete message, "
                        "senderId %d", senderId);
            }

#if PIGGYBACK_SHUFFLE_MSG
            if (senderId != world->rank) {
                respHdr->totalLength = totalLength;
                respHdr->offset = offset;
                rpc->replyPayload->appendExternal(rpc->requestPayload, 0,
                        reqHdr->chunkSize);
            }
#endif
            break;
        }
        default:
            DIE("Unexpected shuffle opcode %u", reqHdr->dataId);
    }
}

void
MilliSortService::benchmarkCollectiveOp(
        const WireFormat::BenchmarkCollectiveOp::Request* reqHdr,
        WireFormat::BenchmarkCollectiveOp::Response* respHdr,
        Rpc* rpc)
{
    static std::atomic<bool> ongoingBenchmark;
    if (reqHdr->masterId == 0) {
        if (ongoingBenchmark) {
            LOG(ERROR, "Already servicing another BenchmarkCollectiveOp RPC!");
            respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
            return;
        }
        ongoingBenchmark = true;
    }

#define WARMUP_COUNT 20
    std::unique_ptr<char[]> data(new char[std::max(1u, reqHdr->dataSize)]);
    if (reqHdr->opcode == WireFormat::SEND_DATA) {
        /* Point-to-point message benchmark */
        ServerId target = world->getNode(world->size() - 1);
        for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
            uint64_t startTime = Cycles::rdtsc();
            SendDataRpc sendDataRpc(context, target, POINT2POINT_BENCHMARK,
                    reqHdr->dataSize, data.get());
            Buffer* result = sendDataRpc.wait();
            if (i >= WARMUP_COUNT) {
                uint64_t remoteEndTime = *result->getStart<uint64_t>();
                uint64_t endTime = context->clockSynchronizer->getConverter(
                        target).toLocalTsc(remoteEndTime);
                uint64_t oneWayDelayNs =
                        Cycles::toNanoseconds(endTime - startTime);
                rpc->replyPayload->emplaceAppend<uint64_t>(oneWayDelayNs);
            }
        }
    } else if (reqHdr->opcode == WireFormat::BCAST_TREE) {
        /* Broadcast benchmark */
        std::vector<uint32_t> latencyNs(world.get()->size() - 1);
        for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
            uint64_t startTime = Cycles::rdtsc();
            TreeBcast bcast(context, world.get(), downCast<uint32_t>(i));
            bcast.send(data.get(), reqHdr->dataSize);
            Buffer* result = bcast.wait();
            if (i >= WARMUP_COUNT) {
                uint32_t offset = 0;
                while (offset < result->size()) {
                    uint32_t rank = *result->read<uint32_t>(&offset);
                    uint64_t endTime = *result->read<uint64_t>(&offset);
                    latencyNs[rank - 1] = downCast<uint32_t>(
                            Cycles::toNanoseconds(endTime - startTime));
                }
                rpc->replyPayload->appendCopy(latencyNs.data(),
                        downCast<uint32_t>(latencyNs.size() * 4));
            }
        }
    } else if (reqHdr->opcode == WireFormat::GATHER_TREE) {
        /* Gather benchmark */

        // The master node is responsible for running many iterations of the
        // experiment and recording the completion time of each run. Note that
        // the experiment relies on clock sync. to ensure all participating
        // nodes invokes/enters the collective operation at the same time. To
        // start a run, the master node broadcasts the BenchmarkCollectiveOpRpc
        // to all participating nodes which includes a starting time. Upon the
        // receipt of the benchmark RPC, a node waits until the starting time
        // to invoke the collective op. The completion time is computed on the
        // gather-tree root node and sends back via the response of benchmark
        // RPC. The outer broadcast op that carries the benchmark RPC then
        // return the completion time as result to the master node.
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();

                // Only one BenchmarkCollectiveOpRpc will have non-zero result
                // (i.e., the one sent to the gather-tree root node).
                uint32_t offset = 0;
                uint64_t gatherNs = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint64_t ns = *result->read<uint64_t>(&offset);
                    if (ns > 0) {
                        gatherNs = ns;
                    }
                }
                if (i >= WARMUP_COUNT) {
                    rpc->replyPayload->emplaceAppend<uint64_t>(gatherNs);
                }
            }
        } else {
            class DataMerger : public TreeGather::Merger {
              public:
                explicit DataMerger(bool isRoot)
                    : bytesReceived()
                    , isRoot(isRoot)
                    , localBuf()
                    , mutex("DataMerger::mutex")
                {}

                void add(Buffer* dataBuf)
                {
                    // Quick hack to skip processing received data at the root
                    // node.
                    if (isRoot) {
                        bytesReceived += downCast<int>(dataBuf->size());
                    } else {
                        SpinLock::Guard _(mutex);
                        localBuf.appendCopy(dataBuf);
                    }
                }

                Buffer* getResult() { return &localBuf; }

                std::atomic<int> bytesReceived;

                bool isRoot;

                Buffer localBuf;

                SpinLock mutex;
            };
            DataMerger merger(world->rank == 0);
            merger.localBuf.appendCopy(data.get(), reqHdr->dataSize);

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "Gather operation started %.2f us late!",
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("Gather operation started, run %d", reqHdr->count);

            const int DUMMY_OP_ID = 999;
            Tub<TreeGather> gatherOp;
            invokeCollectiveOp<TreeGather>(gatherOp, DUMMY_OP_ID, context,
                    world.get(), 0, &merger);
            gatherOp->wait();
            uint64_t endTime = Cycles::rdtsc();
            timeTrace("Gather operation completed");
            // TODO: the cleanup is not very nice; how to do RAII?
            removeCollectiveOp(DUMMY_OP_ID);

            uint64_t gatherNs = 0;
            if (world->rank == 0) {
                gatherNs = Cycles::toNanoseconds(endTime - startTime);
                // Verify if the root has the right amount of data in the end.
                int totalBytes = int(reqHdr->dataSize) + merger.bytesReceived;
                if (totalBytes != world->size() * int(reqHdr->dataSize)) {
                    LOG(ERROR, "Unexpected final data size %d", totalBytes);
                }
            }
            rpc->replyPayload->emplaceAppend<uint64_t>(gatherNs);

        }
    } else if (reqHdr->opcode == WireFormat::ALL_GATHER) {
        /* All-gather benchmark */

        // The master node is responsible for running many iterations of the
        // experiment and recording the completion time of each run. Note that
        // the experiment relies on clock sync. to ensure all participating
        // nodes invokes/enters the collective operation at the same time. To
        // start a run, the master node broadcasts the BenchmarkCollectiveOpRpc
        // to all participating nodes which includes a starting time. Upon the
        // receipt of the benchmark RPC, a node waits until the starting time
        // to invoke the collective op. The completion time is computed on each
        // node independently and sent back via the response of benchmark
        // RPC. The outer broadcast op that carries the benchmark RPC then
        // return the completion times of all nodes to the master node.
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();

                // The completion time for the all-gather operation is the
                // largest completion time on all nodes.
                uint32_t offset = 0;
                uint64_t allGatherNs = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint64_t ns = *result->read<uint64_t>(&offset);
                    if (ns > allGatherNs) {
                        allGatherNs = ns;
                    }
                }
                if (i >= WARMUP_COUNT) {
                    rpc->replyPayload->emplaceAppend<uint64_t>(allGatherNs);
                }
            }
        } else {
            class DataMerger : public AllGather::Merger {
              public:
                explicit DataMerger() : localBuf() {}

                void append(Buffer* dataBuf) { localBuf.appendCopy(dataBuf); }

                void clear() { localBuf.reset(); }

                void fillBuffer(Buffer* out) { out->appendCopy(&localBuf); }

                Buffer localBuf;
            };
            DataMerger merger;
            std::fill(data.get(), data.get() + reqHdr->dataSize,
                    world->rank % 256);
            merger.localBuf.appendExternal(data.get(), reqHdr->dataSize);

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "Gather operation started %.2f us late!",
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("AllGather operation started, run %d", reqHdr->count);

#if 0
            // Randomly sleep 0-100us to test the robustness of the protocol.
            Arachne::sleep((uint64_t(generateRandom()) % 100) * 1000);
#endif
            const int DUMMY_OP_ID = 999;
            Tub<AllGather> allGatherOp;
            invokeCollectiveOp<AllGather>(allGatherOp, DUMMY_OP_ID, context,
                    world.get(), reqHdr->dataSize, data.get(), &merger);
            // For the micro-benchmark, we are going to "cheat" by using the
            // internal merge complete time, as opposed to the time when all
            // outgoing RPCs are done.
//            uint64_t endTime = Cycles::rdtsc();
            uint64_t endTime = allGatherOp->wait();
            uint64_t allGatherNs = Cycles::toNanoseconds(endTime - startTime);
            timeTrace("AllGather operation completed in %lu ns", allGatherNs);
            // TODO: the cleanup is not very nice; how to do RAII?
            removeCollectiveOp(DUMMY_OP_ID);
            rpc->replyPayload->emplaceAppend<uint64_t>(allGatherNs);

            // Verify if we have the right amount of data in the end.
            if (merger.localBuf.size() !=
                    uint32_t(world->size()) * reqHdr->dataSize) {
                LOG(ERROR, "Unexpected final data size %u",
                        merger.localBuf.size());
                timeTrace("Unexpected final data size %d",
                        merger.localBuf.size());
            } else {
                uint32_t offset = 0;
                bool checkFailed = false;
                for (int i = 0; i < world->size(); i++) {
                    char expected = *merger.localBuf.read<char>(&offset);
                    for (uint32_t j = 1; j < reqHdr->dataSize; j++) {
                        char actual = *merger.localBuf.read<char>(&offset);
                        if (expected != actual) {
                            timeTrace("All-gather result corrupted at index "
                                    "%u, expected %u, actual %u", offset - 1,
                                    expected, actual);
                            checkFailed = true;
                        }
                    }
                }
                if (checkFailed) {
                    LOG(ERROR, "All-gather result corrupted");
                }
            }
        }
    } else if (reqHdr->opcode == WireFormat::SHUFFLE_PUSH) {
        /* Shuffle benchmark */
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();
                uint64_t elapsedMicros =
                        Cycles::toMicroseconds(Cycles::rdtsc() - startTime);

                // Take the completion time of the slowest node as the
                // completion time of the shuffle operation.
                uint32_t offset = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint32_t rank = *result->read<uint32_t>(&offset);
                    uint32_t shuffleNs = *result->read<uint32_t>(&offset);
                    if (i >= WARMUP_COUNT) {
                        // Note: the local elapsed time is always larger than the
                        // the shuffle completion time because the former also
                        // includes the time to send/receive the payload RPC reply
                        // (note: the payload RPC of bcast is currently delivered
                        // over the network even though it's actually a local op)
                        // and the broadcast return time.
                        LOG(DEBUG, "Master node elapsed time %lu us, "
                                "shuffle time %u us", elapsedMicros,
                                shuffleNs / 1000);
                        rpc->replyPayload->emplaceAppend<uint32_t>(rank);
                        rpc->replyPayload->emplaceAppend<uint32_t>(shuffleNs);
                    }
                }
            }
        } else {
            std::vector<Buffer> messages(world->size());
            std::unique_ptr<char[]> space(new char[8000000]);
            Buffer outMessage;
            for (int i = 0; i < world->size(); i++) {
                messages[i].appendExternal(space.get(), reqHdr->dataSize);
            }

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "Shuffle operation %d started %.2f us late!",
                        reqHdr->count,
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("Shuffle benchmark started, run %d", reqHdr->count);
            invokeShufflePush(world.get(), ALLSHUFFLE_BENCHMARK, &messages);
            uint32_t shuffleNs = downCast<uint32_t>(
                    Cycles::toNanoseconds(Cycles::rdtsc() - startTime));
            timeTrace("Shuffle benchmark run %d completed in %u us, "
                    "sent %u bytes", reqHdr->count, shuffleNs / 1000,
                    reqHdr->dataSize * uint32_t(world->size()));

            rpc->replyPayload->emplaceAppend<uint32_t>(world->rank);
            rpc->replyPayload->emplaceAppend<uint32_t>(shuffleNs);
        }
    } else {
        respHdr->common.status = STATUS_INVALID_PARAMETER;
    }

    ongoingBenchmark = false;
}

void
MilliSortService::sendData(const WireFormat::SendData::Request* reqHdr,
            WireFormat::SendData::Response* respHdr, Rpc* rpc)
{
    switch (reqHdr->dataId) {
        // TODO: hmm, right now we handle incoming data differently in Bcast &
        // gather; in gather, we use merger to incorporate incoming data and
        // no need for SendDataRpc, while here we use SendData RPC handler to
        // to incorporate the data. Think about how to make it uniform.
        case BROADCAST_PIVOT_BUCKET_BOUNDARIES: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (world->rank > 0) {
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                     offset < request->size();) {
                    pivotBucketBoundaries.push_back(
                            *request->read<PivotKey>(&offset));
                }
                timeTrace("received %lu pivot bucket boundaries",
                        pivotBucketBoundaries.size());

                rpc->sendReply();
                pivotBucketSort();
            }
            break;
        }
        case BROADCAST_DATA_BUCKET_BOUNDARIES: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (!isPivotServer) {
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                     offset < request->size();) {
                    // Put into splitters directly.
                    splitters.push_back(*request->read<PivotKey>(&offset));
                }
                timeTrace("received %lu splitters", splitters.size());
                debugLogKeys("splitters: ", &splitters);

                rpc->sendReply();
                shuffleRecords();
            }
            break;
        }
        case BROADCAST_SHAKESPEARE_WORDS: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (world->rank > 0) {
                assert(bigQueryQ4UniqueWords.empty());
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                        offset < request->size();) {
                    bigQueryQ4UniqueWords.emplace_back("");
                    std::string& word = bigQueryQ4UniqueWords.back();
                    while (char ch = *request->read<char>(&offset)) {
                        word.append(1, ch);
                    }
                }
                bigQueryQ4BcastDone = true;
                rpc->sendReply();
            }
            break;
        }
        case POINT2POINT_BENCHMARK: {
            // Return the local time back to the sender so it can compute the
            // one-way delay using synchronized clock.
            rpc->replyPayload->emplaceAppend<uint64_t>(Cycles::rdtsc());
        }
        default:
            assert(false);
    }
}

} // namespace RAMCloud
